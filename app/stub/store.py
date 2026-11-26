"""内存存储：模拟 etcd 中残留 v1beta1 编码对象的节点维护策略 CR。

语义对齐真实 apiserver 的本任务相关子集：
- GET / list 按请求版本服务端转换，对象内部保留实际存储版本标记；
- PUT 带 resourceVersion 乐观并发；写回后存储版本变为 v1；
- status 子资源：PUT status 不改 spec，spec 写回不改 status；
- 全局 resourceVersion 单调递增；
- 可注入故障钩子（500、409、并发改策略、遍历中删除）用于演示与测试。
"""
from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from copy import deepcopy
from typing import Any

from . import CRD_NAME, GROUP, PLURAL, V1, V1BETA1
from .conversion import body_to_internal, convert_object


class ApiException(Exception):
    def __init__(self, status: int, reason: str, message: str):
        super().__init__(message)
        self.status = status
        self.reason = reason
        self.message = message

    def body(self) -> dict:
        # 模仿 k8s Status 对象
        return {
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "reason": self.reason,
            "message": self.message,
            "code": self.status,
        }


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _crd_object(stored_versions: list[str]) -> dict:
    return {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": CRD_NAME,
            "uid": str(uuid.uuid4()),
            "resourceVersion": "1",
            "generation": 1,
            "creationTimestamp": _now(),
        },
        "spec": {
            "group": GROUP,
            "scope": "Cluster",
            "names": {
                "plural": PLURAL,
                "singular": "nodemaintenancepolicy",
                "kind": "NodeMaintenancePolicy",
                "listKind": "NodeMaintenancePolicyList",
            },
            "versions": [
                {
                    "name": V1,
                    "served": True,
                    "storage": True,
                },
                {
                    "name": V1BETA1,
                    "served": True,
                    "storage": False,
                    # 模拟历史：v1beta1 曾是存储版本，仍残留在 status.storedVersions
                },
            ],
        },
        "status": {
            "conditions": [
                {
                    "type": "Established",
                    "status": "True",
                    "lastTransitionTime": _now(),
                    "reason": "InitialDepsSatisfied",
                    "message": "no conflicts found",
                }
            ],
            "storedVersions": list(stored_versions),
        },
    }


def _seed_policies() -> dict[str, dict]:
    """三份历史对象：etcd 中仍以 v1beta1 编码（storage_version=v1beta1）。

    内部统一以 v1 规范化形状保存 spec；v1beta1 的字段形状（drainTimeout
    字符串）仅在经 v1beta1 路径对外服务时由 conversion 转换出来。
    """
    seeds = [
        {
            "name": "default-maintenance",
            "spec": {
                "drainTimeoutSeconds": 30,
                "maxUnavailable": 1,
                "nodeSelector": {"role": "worker"},
                "maintenanceWindow": "Sat 02:00-04:00",
            },
            "status": {
                "phase": "Ready",
                "lastApplied": "2026-11-20T02:00:00Z",
                "observedGeneration": 2,
            },
        },
        {
            "name": "edge-nodes",
            "spec": {
                "drainTimeoutSeconds": 120,
                "maxUnavailable": 2,
                "nodeSelector": {"zone": "edge"},
                "maintenanceWindow": "Sun 01:00-03:00",
            },
            "status": {
                "phase": "Draining",
                "lastApplied": "2026-11-22T01:00:00Z",
                "observedGeneration": 5,
            },
        },
        {
            "name": "database-keepers",
            "spec": {
                "drainTimeoutSeconds": 300,
                "maxUnavailable": 1,
                "nodeSelector": {"workload": "database"},
                "maintenanceWindow": "Sun 03:00-05:00",
            },
            "status": {
                "phase": "Ready",
                "lastApplied": "2026-11-15T03:00:00Z",
                "observedGeneration": 1,
            },
        },
    ]
    objects: dict[str, dict] = {}
    rv = 100
    for seed in seeds:
        rv += 1
        name = seed["name"]
        internal = {
            "group": GROUP,
            "metadata": {
                "name": name,
                "uid": str(uuid.uuid4()),
                "resourceVersion": str(rv),
                "generation": 1,
                "creationTimestamp": _now(),
                "managedFields": [
                    {
                        "manager": "ops-controller",
                        "operation": "Update",
                        "apiVersion": f"{GROUP}/{V1BETA1}",
                    }
                ],
            },
            "spec": seed["spec"],
            "status": seed["status"],
        }
        # 内部以 v1 规范化表示，另用独立字段记录 etcd 实际编码版本
        objects[name] = {"storage_version": V1BETA1, "object": internal}
    return objects


class StubStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._global_rv = 200
        self.reset()

    # ------------------------------------------------------------------ setup
    def reset(self) -> None:
        """恢复初始种子数据（同步，仅在无并发请求的控制接口场景使用）。"""
        self._objects = _seed_policies()
        self._crd = _crd_object([V1BETA1, V1])
        self._global_rv = 200
        # 故障钩子：见 README/控制接口说明
        self.hooks: dict[str, Any] = {}

    # ------------------------------------------------------------- 分页 token
    @staticmethod
    def encode_continue(offset: int) -> str:
        return base64.urlsafe_b64encode(
            json.dumps({"start": offset, "rv": "snapshot"}).encode()
        ).decode()

    @staticmethod
    def decode_continue(token: str) -> int:
        try:
            payload = json.loads(base64.urlsafe_b64decode(token.encode()))
            return int(payload["start"])
        except Exception as exc:  # noqa: BLE001
            raise ApiException(400, "BadRequest", f"invalid continue token: {token}") from exc

    # --------------------------------------------------------------- 故障钩子
    def _maybe_hook(self, kind: str, name: str | None = None) -> None:
        hook = self.hooks.get(kind)
        if not hook:
            return
        if hook.get("used") and not hook.get("repeat"):
            return
        if name is not None and hook.get("name") not in (None, name):
            return
        if "remaining" in hook:
            hook["remaining"] -= 1
            if hook["remaining"] > 0:
                return
        if not hook.get("repeat"):
            hook["used"] = True
        status = hook.get("status", 500)
        raise ApiException(
            status,
            hook.get("reason", "InternalError"),
            hook.get("message", f"injected {kind} failure"),
        )

    # ------------------------------------------------------------------- CRD
    async def get_crd(self) -> dict:
        async with self._lock:
            return deepcopy(self._crd)

    async def put_crd_status(self, body: dict) -> dict:
        async with self._lock:
            rv = body.get("metadata", {}).get("resourceVersion")
            if rv != self._crd["metadata"]["resourceVersion"]:
                raise ApiException(
                    409,
                    "Conflict",
                    f"Operation cannot be fulfilled on customresourcedefinitions \"{CRD_NAME}\": "
                    "the object has been modified; please apply your changes to the latest version",
                )
            stored = body.get("status", {}).get("storedVersions")
            if not isinstance(stored, list) or not stored:
                raise ApiException(
                    422, "Invalid", "status.storedVersions must be a non-empty array"
                )
            # 仅允许收敛到 v1（替身只模拟这一条迁移路径）
            if any(v not in (V1, V1BETA1) for v in stored):
                raise ApiException(422, "Invalid", f"unexpected storedVersions: {stored}")
            self._bump_rv_locked()
            self._crd["status"]["storedVersions"] = list(stored)
            self._crd["metadata"]["resourceVersion"] = str(self._global_rv)
            return deepcopy(self._crd)

    # ------------------------------------------------------------- CR list/get
    async def list_objects(self, limit: int, cont: str | None) -> dict:
        async with self._lock:
            # 注入：首次 list 之后、第二次 list 之前，对象被外部删除
            hook = self.hooks.get("deleteOnFirstList")
            if hook and not hook.get("used") and cont:
                hook["used"] = True
                target = hook.get("name")
                if target in self._objects:
                    self._objects.pop(target)
            self._maybe_hook("list500")
            start = self.decode_continue(cont) if cont else 0
            names = sorted(self._objects)
            page = names[start:start + limit]
            items = [
                convert_object(self._objects[n]["object"], V1) for n in page
            ]
            next_token = None
            if start + limit < len(names):
                next_token = self.encode_continue(start + limit)
            return {
                "apiVersion": f"{GROUP}/{V1}",
                "kind": "NodeMaintenancePolicyList",
                "metadata": {"resourceVersion": str(self._global_rv)},
                "items": items,
                **({"continue": next_token} if next_token else {}),
            }

    async def get_object(self, name: str, version: str = V1) -> dict:
        async with self._lock:
            entry = self._objects.get(name)
            if entry is None:
                raise ApiException(
                    404, "NotFound", f"nodemaintenancepolicies.ops.example.com \"{name}\" not found"
                )
            return convert_object(entry["object"], version)

    # ----------------------------------------------------------------- CR PUT
    async def put_object(self, body: dict, name: str) -> dict:
        async with self._lock:
            entry = self._objects.get(name)
            if entry is None:
                raise ApiException(
                    404, "NotFound", f"nodemaintenancepolicies.ops.example.com \"{name}\" not found"
                )
            current = entry["object"]
            rv = body.get("metadata", {}).get("resourceVersion")
            if rv is None or rv != current["metadata"]["resourceVersion"]:
                raise ApiException(
                    409,
                    "Conflict",
                    f"Operation cannot be fulfilled on nodemaintenancepolicies \"{name}\": "
                    "the object has been modified; please apply your changes to the latest "
                    "version and try again",
                )
            self._maybe_hook("put500", name)
            internal = body_to_internal(body)
            if internal["metadata"].get("name") != name:
                raise ApiException(422, "Invalid", "metadata.name must match path")

            # 注入：并发控制器在本次写回前先改一次策略（模拟真实并发写）
            hook = self.hooks.get("concurrentEdit")
            if hook and not hook.get("used") and hook.get("name") in (None, name):
                hook["used"] = True
                patch = hook["patch"]
                current["spec"].update(deepcopy(patch))
                current["metadata"]["generation"] += 1
                self._bump_rv_locked()
                current["metadata"]["resourceVersion"] = str(self._global_rv)
                raise ApiException(
                    409,
                    "Conflict",
                    f"nodemaintenancepolicies \"{name}\" concurrently modified by ops-controller",
                )

            # 注入：本次写回必失败一次然后恢复（重试成功路径）
            fail_hook = self.hooks.get("put500Once")
            if fail_hook and not fail_hook.get("used") and fail_hook.get("name") in (None, name):
                fail_hook["used"] = True
                raise ApiException(
                    500, "InternalError", f"injected transient write failure on {name}"
                )

            # status 子资源语义：普通写回不允许改 status，保留库里的 status
            new_spec = internal["spec"]
            current["spec"] = new_spec
            current["metadata"]["generation"] = internal["metadata"].get(
                "generation", current["metadata"]["generation"]
            )
            self._bump_rv_locked()
            current["metadata"]["resourceVersion"] = str(self._global_rv)
            # 关键：通过 v1 路径写回 => 按 v1 重新编码落盘
            entry["storage_version"] = V1
            return convert_object(current, V1)

    async def put_object_status(self, body: dict, name: str) -> dict:
        async with self._lock:
            entry = self._objects.get(name)
            if entry is None:
                raise ApiException(404, "NotFound", f"\"{name}\" not found")
            current = entry["object"]
            rv = body.get("metadata", {}).get("resourceVersion")
            if rv is None or rv != current["metadata"]["resourceVersion"]:
                raise ApiException(409, "Conflict", f"\"{name}\" has been modified")
            if "status" not in body:
                raise ApiException(422, "Invalid", "status body required")
            current["status"] = deepcopy(body["status"])
            self._bump_rv_locked()
            current["metadata"]["resourceVersion"] = str(self._global_rv)
            entry["storage_version"] = V1
            return convert_object(current, V1)

    # -------------------------------------------------------------- CR delete
    async def delete_object(self, name: str) -> dict:
        async with self._lock:
            if name not in self._objects:
                raise ApiException(404, "NotFound", f"\"{name}\" not found")
            self._objects.pop(name)
            return {"kind": "Status", "apiVersion": "v1", "status": "Success", "code": 200}

    # ----------------------------------------------------------- 控制/观测面
    async def storage_view(self) -> dict:
        async with self._lock:
            items = []
            for name in sorted(self._objects):
                entry = self._objects[name]
                items.append(
                    {
                        "name": name,
                        "storageVersion": entry["storage_version"],
                        "resourceVersion": entry["object"]["metadata"]["resourceVersion"],
                        "generation": entry["object"]["metadata"]["generation"],
                        "spec": convert_object(entry["object"], V1)["spec"],
                        "status": deepcopy(entry["object"]["status"]),
                    }
                )
            return {
                "storedVersions": list(self._crd["status"]["storedVersions"]),
                "objects": items,
                "hooks": deepcopy(self.hooks),
            }

    async def seed_v1beta1(self, body: dict) -> dict:
        """控制接口：直接以 v1beta1 编码写入一个对象（模拟历史写入路径）。"""
        async with self._lock:
            internal = body_to_internal(body)
            name = internal["metadata"]["name"]
            if name in self._objects:
                raise ApiException(409, "Conflict", f"\"{name}\" already exists")
            self._bump_rv_locked()
            internal["metadata"].setdefault("uid", str(uuid.uuid4()))
            internal["metadata"]["resourceVersion"] = str(self._global_rv)
            internal["metadata"].setdefault("generation", 1)
            internal["metadata"].setdefault("creationTimestamp", _now())
            internal.setdefault("status", {})
            self._objects[name] = {"storage_version": V1BETA1, "object": internal}
            return convert_object(internal, V1)

    async def set_hooks(self, hooks: dict) -> dict:
        async with self._lock:
            self.hooks = deepcopy(hooks)
            return deepcopy(self.hooks)

    # ------------------------------------------------------------------ 工具
    def _bump_rv_locked(self) -> None:
        self._global_rv += 1


store = StubStore()
