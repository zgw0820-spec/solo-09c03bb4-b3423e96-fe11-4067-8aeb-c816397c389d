"""节点维护策略（NodeMaintenancePolicy）集群级自定义资源。

- API 组: ops.example.com
- 服务版本: v1（迁移期间唯一被客户端使用的版本）、v1beta1（历史版本）
- 存储版本固定为 v1（CRD spec.versions[*].storage 只有 v1 为 true）
- 作用域: Cluster（集群级，无 namespace）

替身模拟 etcd 中残留的 v1beta1 编码对象：对象在内部以 v1 规范化表示，
额外记录 ``storage_version``；通过 v1 路径读取时由服务端做版本转换，
v1 PUT 写回后 storage_version 变为 v1 —— 这正是真实 apiserver 在
存储版本迁移中把对象按新存储版本重新落盘的行为。
"""

CRD_NAME = "nodemaintenancepolicies.ops.example.com"
GROUP = "ops.example.com"
PLURAL = "nodemaintenancepolicies"
V1 = "v1"
V1BETA1 = "v1beta1"
STORAGE_VERSION = V1
