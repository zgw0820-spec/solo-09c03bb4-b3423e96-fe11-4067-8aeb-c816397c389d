"""v1beta1 与 v1 之间的服务端版本转换（模拟 apiserver 的 conversion webhook）。

字段差异（故意保留一个真实的 schema 差异，证明转换由服务端完成）：
- v1beta1.spec.drainTimeout:    带单位字符串，如 "30s"、"2m"
- v1.spec.drainTimeoutSeconds:  整数秒
其余字段两版同名同义；status 在两版间结构相同、原样保留。
"""
from __future__ import annotations

from copy import deepcopy

from . import V1, V1BETA1

_DURATIONS = {"s": 1, "m": 60, "h": 3600}


def _parse_duration(value: str) -> int:
    value = value.strip()
    unit = value[-1:]
    if unit in _DURATIONS:
        return int(value[:-1]) * _DURATIONS[unit]
    # 裸数字按秒处理
    return int(value)


def _format_duration(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _convert(spec: dict, src: str, dst: str) -> dict:
    spec = deepcopy(spec)
    if src == V1BETA1 and dst == V1:
        if "drainTimeout" in spec:
            spec["drainTimeoutSeconds"] = _parse_duration(spec.pop("drainTimeout"))
    elif src == V1 and dst == V1BETA1:
        if "drainTimeoutSeconds" in spec:
            spec["drainTimeout"] = _format_duration(spec.pop("drainTimeoutSeconds"))
    return spec


def convert_object(obj: dict, target_version: str) -> dict:
    """把内部 v1 对象转换为请求版本的表示，不修改入参。"""
    out = deepcopy(obj)
    if target_version == V1:
        out["apiVersion"] = f"{obj['group']}/{V1}"
        out["kind"] = "NodeMaintenancePolicy"
        return out
    if target_version == V1BETA1:
        out["apiVersion"] = f"{obj['group']}/{V1BETA1}"
        out["kind"] = "NodeMaintenancePolicy"
        out["spec"] = _convert(out["spec"], V1, V1BETA1)
        return out
    raise ValueError(f"unsupported version {target_version!r}")


def body_to_internal(body: dict) -> dict:
    """把客户端提交的对象体规范化为内部 v1 表示。"""
    api_version = body.get("apiVersion", "")
    if "/" in api_version:
        group, version = api_version.split("/", 1)
    else:
        group, version = "", api_version
    if group != "ops.example.com" or version not in (V1, V1BETA1):
        raise ValueError(f"unsupported apiVersion {api_version!r}")
    if body.get("kind") != "NodeMaintenancePolicy":
        raise ValueError(f"unsupported kind {body.get('kind')!r}")

    internal = deepcopy(body)
    internal["group"] = group
    internal.pop("apiVersion", None)
    internal.pop("kind", None)
    if version == V1BETA1:
        internal["spec"] = _convert(internal["spec"], V1BETA1, V1)
    return internal
