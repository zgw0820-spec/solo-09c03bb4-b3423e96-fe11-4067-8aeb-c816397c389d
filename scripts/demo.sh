#!/usr/bin/env bash
# 端到端演示：旧 v1beta1 对象改存为 v1，含并发改策略、遍历中删除、故障重跑。
#
# 用法（compose 已启动后）:
#   ./scripts/demo.sh
# 也可指向自定义地址:
#   MIGRATOR=http://127.0.0.1:18000 STUB=http://127.0.0.1:18080 ./scripts/demo.sh
set -euo pipefail

MIGRATOR="${MIGRATOR:-http://127.0.0.1:18000}"
STUB="${STUB:-http://127.0.0.1:18080}"

p() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
wait_api() { # $1=url
  for _ in $(seq 1 30); do
    curl -fsS "$1/healthz" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "service not ready: $1" >&2
  exit 1
}

wait_api "$STUB"
wait_api "$MIGRATOR"

p "0. 重置替身到初始状态（3 个对象均以 v1beta1 编码）"
curl -fsS -X POST "$STUB/__control/reset" | jq .
curl -fsS "$STUB/__control/storage" | jq '{storedVersions, objects: [.objects[] | {name, storageVersion}]}'

p "1. 正常改存：分页 LIST v1 -> 原样 PUT 回 v1 -> 移除 storedVersions 里的 v1beta1"
curl -fsS -X POST "$MIGRATOR/migrate" | jq .
curl -fsS "$STUB/__control/storage" | jq '{storedVersions, objects: [.objects[] | {name, storageVersion}]}'

p "2. 幂等重跑：再次执行仍成功，且不会重复删标记"
curl -fsS -X POST "$MIGRATOR/migrate" | jq '{status, legacyVersionRemoved, storedVersionsAfter, objectsRewritten}'

p "3. 并发改策略：控制器在改存期间修改 edge-nodes，迁移遇 409 重读最新对象再试"
curl -fsS -X POST "$STUB/__control/reset" >/dev/null
curl -fsS -X PUT "$STUB/__control/hooks" -H 'Content-Type: application/json' \
  -d '{"concurrentEdit": {"name": "edge-nodes", "patch": {"maxUnavailable": 7}}}' | jq .
curl -fsS -X POST "$MIGRATOR/migrate" \
  | jq '{status, results: [.results[] | select(.name=="edge-nodes")]}'
# 并发修改必须被保留，且对象最终按 v1 落盘
curl -fsS "$STUB/__control/storage" \
  | jq '.objects[] | select(.name=="edge-nodes") | {name, storageVersion, maxUnavailable: .spec.maxUnavailable}'

p "4. 遍历中删除：第二页翻页前 edge-nodes 被删除，迁移跳过且不补建，整体仍成功"
curl -fsS -X POST "$STUB/__control/reset" >/dev/null
curl -fsS -X PUT "$STUB/__control/hooks" -H 'Content-Type: application/json' \
  -d '{"deleteOnFirstList": {"name": "edge-nodes"}}' >/dev/null
curl -fsS -X POST "$MIGRATOR/migrate" | jq '{status, objectsListed, objectsRewritten, storedVersionsAfter}'
curl -fsS "$STUB/__control/storage" | jq '[.objects[].name]'

p "5. 写回持续故障：保留 v1beta1 标记并指出出错对象；恢复后重跑即收敛"
curl -fsS -X POST "$STUB/__control/reset" >/dev/null
curl -fsS -X PUT "$STUB/__control/hooks" -H 'Content-Type: application/json' \
  -d '{"put500": {"name": "edge-nodes", "repeat": true, "message": "disk full"}}' >/dev/null
curl -fsS -X POST "$MIGRATOR/migrate" | jq '{status, error, legacyVersionRemoved}'
curl -fsS -X PUT "$STUB/__control/hooks" -H 'Content-Type: application/json' -d '{}' >/dev/null
curl -fsS -X POST "$MIGRATOR/migrate" | jq '{status, storedVersionsAfter, objectsRewritten}'

p "演示完成"
