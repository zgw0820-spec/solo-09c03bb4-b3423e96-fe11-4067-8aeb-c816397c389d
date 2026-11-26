"""本地 Kubernetes API 替身（只覆盖本任务用到的端点）。

业务端点（模拟 apiserver）
  GET    /apis/apiextensions.k8s.io/v1/customresourcedefinitions/<crd>
  PUT    /apis/apiextensions.k8s.io/v1/customresourcedefinitions/<crd>/status
  GET    /apis/ops.example.com/v1/nodemaintenancepolicies
  GET    /apis/ops.example.com/v1/nodemaintenancepolicies/{name}
  GET    /apis/ops.example.com/v1beta1/nodemaintenancepolicies/{name}   (老版本仍 served)
  PUT    /apis/ops.example.com/v1/nodemaintenancepolicies/{name}
  PUT    /apis/ops.example.com/v1/nodemaintenancepolicies/{name}/status
  DELETE /apis/ops.example.com/v1/nodemaintenancepolicies/{name}

控制端点（演示/测试用，不是迁移接口）
  POST   /__control/reset                       恢复种子数据并清空钩子
  GET    /__control/storage                     查看每个对象的实际存储版本、
                                                CRD UID/conditions/状态写次数等
  POST   /__control/seed-v1beta1                以 v1beta1 编码直接写入对象
  PUT    /__control/hooks                       注入故障/并发钩子
    （含 crdStatusConflictOnce：CRD /status 409 竞争；
      crdRecreateOnce：同名 CRD 重建导致 UID 变化）
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import CRD_NAME, GROUP, PLURAL, V1, V1BETA1
from .store import ApiException, store

app = FastAPI(title="k8s-api-stub", version="1.0.0")


@app.exception_handler(ApiException)
async def api_exception_handler(_: Request, exc: ApiException) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content=exc.body())


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


# ---------------------------------------------------------------------- CRD
@app.get(f"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{CRD_NAME}")
async def get_crd() -> dict:
    return await store.get_crd()


@app.put(f"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{CRD_NAME}/status")
async def put_crd_status(request: Request) -> dict:
    return await store.put_crd_status(await request.json())


# ------------------------------------------------------------- CR 业务端点
_BASE = f"/apis/{GROUP}/{V1}/{PLURAL}"


@app.get(_BASE)
async def list_policies(request: Request, limit: int = 500) -> dict:
    # apiserver 的分页参数名为 continue（Python 关键字），从 querystring 直取
    cont = request.query_params.get("continue")
    return await store.list_objects(limit=limit, cont=cont)


@app.get(f"{_BASE}/{{name}}")
async def get_policy(name: str) -> dict:
    return await store.get_object(name, V1)


# v1beta1 仍被 served：用于直接观察老版本表示（转换由服务端完成）。
# 迁移服务不会调用此路径——它固定只走 v1。
@app.get(f"/apis/{GROUP}/{V1BETA1}/{PLURAL}/{{name}}")
async def get_policy_v1beta1(name: str) -> dict:
    return await store.get_object(name, V1BETA1)


@app.put(f"{_BASE}/{{name}}")
async def put_policy(name: str, request: Request) -> dict:
    return await store.put_object(await request.json(), name)


@app.put(f"{_BASE}/{{name}}/status")
async def put_policy_status(name: str, request: Request) -> dict:
    return await store.put_object_status(await request.json(), name)


@app.delete(f"{_BASE}/{{name}}")
async def delete_policy(name: str) -> dict:
    return await store.delete_object(name)


# ------------------------------------------------------------- 控制端点
@app.post("/__control/reset")
async def reset() -> dict:
    store.reset()
    return {"status": "reset"}


@app.get("/__control/storage")
async def storage_view() -> dict:
    return await store.storage_view()


@app.post("/__control/seed-v1beta1")
async def seed_v1beta1(request: Request) -> dict:
    return await store.seed_v1beta1(await request.json())


@app.put("/__control/hooks")
async def set_hooks(request: Request) -> dict:
    return await store.set_hooks(await request.json())
