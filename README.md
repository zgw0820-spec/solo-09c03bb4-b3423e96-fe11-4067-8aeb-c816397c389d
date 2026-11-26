# 节点维护策略存储版本改存服务

节点维护策略自定义资源（`NodeMaintenancePolicy`，集群级，API 组 `ops.example.com`）
的存储版本已切换为 **v1**，但 etcd 中可能仍残留 **v1beta1** 编码的老对象。
本项目提供一个 FastAPI 改存接口，通过 **v1 API 分页读取再原样写回**，让
Kubernetes apiserver 按当前存储版本 v1 把对象重新落盘，最终把 `v1beta1`
从 CRD 的 `status.storedVersions` 中移除。

## 设计约束与对应实现

| 约束 | 实现 |
| --- | --- |
| 只处理这一种集群级 CR | 路径、group/plural 全部硬编码；启动迁移前校验 CRD `scope=Cluster`、group 匹配 |
| 迁移期间存储版本固定 v1 | 所有 CR 请求只走 `/apis/ops.example.com/v1/...`；前置校验 `storage=true` 的版本唯一且为 v1，否则拒绝迁移 |
| 分页从 v1 读对象 | `GET .../v1/nodemaintenancepolicies?limit=&continue=` 翻页直到无 `continue` token |
| 让 K8s 实际按 v1 写回 | 对每个对象 `GET` 到的 v1 表示**原样 PUT**（仅携带读到的完整对象与 resourceVersion），由服务端按存储版本重新编码；不增删字段 |
| 不能改策略内容或状态 | 不修改 `spec`/`status` 任何字节；普通写回走主资源端点，替身按 k8s 语义保留 `status` 子资源不被主资源写覆盖 |
| 版本冲突读最新再试 | PUT 收到 409 → 重新 GET 最新对象（全新 resourceVersion）后重试，上限 `MAX_CONFLICT_RETRIES`，退避递增 |
| 已删跳过、不补建 | GET/PUT 收到 404 → 记 `skipped-deleted`，不执行任何创建动作 |
| 全部成功才删旧标记 | 只有遍历完整 **且** 无失败对象时，才 PUT CRD `/status` 移除 `v1beta1`；CRD status 自身 409 也重读重试 |
| 失败保留旧标记并指出出错处 | LIST 失败/对象写回失败/CRD 状态更新失败均直接返回 `status=failed` 报告，含 `error.stage`、出错对象名、分页位置与重跑提示；`storedVersions` 保持不动 |
| 可安全重跑 | 流程幂等：已改存对象再写一次无害；旧标记已移除时只做对象改存不再动 CRD |

## 目录结构

```
app/
  config.py            环境变量配置（API 地址、分页大小、重试上限）
  k8s.py               最小 Kubernetes REST 客户端（httpx，含 5xx 重试）
  migrator.py          改存核心流程与结构化报告
  main.py              FastAPI 改存服务（POST /migrate、GET /migrate/last、/healthz）
  stub/
    conversion.py      v1beta1 <-> v1 服务端转换（drainTimeout 字符串 <-> 整数秒）
    store.py           内存存储：分页、resourceVersion 乐观锁、status 子资源、故障钩子
    app.py             Kubernetes API 替身（FastAPI），含演示控制端点
tests/                 16 个 pytest 用例（ASGI in-process，无需端口）
scripts/demo.sh        端到端演示脚本
docker-compose.yml     替身(18080) + 迁移服务(18000)
Dockerfile             单镜像，按 command 启动两个角色
```

## Docker Compose 启动

```bash
docker compose up --build
# 迁移服务:  http://127.0.0.1:18000
# API 替身:  http://127.0.0.1:18080
```

另开终端运行演示（旧对象改存、并发改策略、遍历中删除、故障重跑）：

```bash
./scripts/demo.sh
```

## 接口说明

### 改存服务（:18000）

- `POST /migrate` — 执行一次完整改存，同步返回报告。业务失败返回 HTTP 200 +
  `"status": "failed"`（错误详情在 `error` 字段，便于脚本解析）；已有迁移在跑时返回 409。
- `GET /migrate/last` — 最近一次报告。
- `GET /healthz` — 健康检查。

报告字段：`storedVersionsBefore/After`、`pagesProcessed`、`objectsListed`、
`objectsRewritten`、`objectsSkippedDeleted`、`objectsFailed`、逐对象 `results`
（含尝试次数、冲突重试次数）、`error.stage/message/hint`。

### Kubernetes API 替身（:18080）

业务端点与真实 apiserver 同形：CRD GET、CRD `/status` PUT、CR 的
list/get/put/status/delete。另有演示控制端点（真实集群中不存在）：

- `POST /__control/reset` — 恢复 3 个 v1beta1 编码种子对象
- `GET  /__control/storage` — 查看每个对象**实际存储版本**、spec/status 与 CRD storedVersions
- `POST /__control/seed-v1beta1` — 以 v1beta1 编码直接写入对象
- `PUT  /__control/hooks` — 注入故障/并发：
  - `concurrentEdit`：指定对象首次 PUT 时模拟控制器并发改 spec 并返回 409
  - `deleteOnFirstList`：翻第二页前删除指定对象
  - `put500Once`：指定对象首次写回一次性 500（验证 5xx 重试）
  - `put500` + `repeat:true`：指定对象持续 500（验证失败保留旧标记）
  - `list500` + `repeat:true`：LIST 持续失败

## 手工请求示例

```bash
# 初始状态：三个对象 storageVersion 全是 v1beta1
curl -s http://127.0.0.1:18080/__control/storage | jq '.storedVersions, [.objects[].storageVersion]'

# 执行改存
curl -s -X POST http://127.0.0.1:18000/migrate | jq .

# 并发改策略演示：注入 409 后再迁移
curl -s -X PUT http://127.0.0.1:18080/__control/hooks -H 'Content-Type: application/json' \
  -d '{"concurrentEdit": {"name": "edge-nodes", "patch": {"maxUnavailable": 7}}}'
curl -s -X POST http://127.0.0.1:18000/migrate | jq '.results, .status'
```

## 本地无 Docker 运行（本仓库实测方式）

```bash
python3 -m uvicorn app.stub.app:app --port 18080 &
KUBERNETES_API_URL=http://127.0.0.1:18080 PAGE_LIMIT=2 \
  python3 -m uvicorn app.main:app --port 18000
python3 -m pytest tests/ -q
./scripts/demo.sh
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `KUBERNETES_API_URL` | `http://127.0.0.1:8080` | apiserver/替身地址 |
| `PAGE_LIMIT` | `500` | 分页大小（compose 与测试中设为 2 以强制翻页） |
| `MAX_CONFLICT_RETRIES` | `5` | 409 冲突重读重试上限（对象与 CRD status 共用） |
| `MAX_SERVER_RETRIES` | `3` | 5xx/传输层错误的 HTTP 客户端重试上限 |
| `RETRY_BACKOFF_SECONDS` | `0.2` | 线性退避基数 |

## 与真实集群对接时的差异

替身只实现本任务所需语义。接入真实 apiserver 时：
`KubernetesClient` 改用 kubeconfig/ServiceAccount 认证（SA 需该 CR 的
get/list/update 与 CRD 的 get/update/status 权限）；转换由 apiserver 的
conversion webhook 完成；其余迁移流程与报告语义不变。
