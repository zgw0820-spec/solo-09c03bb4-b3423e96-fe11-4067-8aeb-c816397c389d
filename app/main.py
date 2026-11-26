"""FastAPI 改存服务入口。

端点：
  GET  /healthz       健康检查
  POST /migrate       执行一次节点维护策略存储版本改存（同步返回完整报告）
  GET  /migrate/last  查看最近一次迁移报告（供运维排查/重跑确认）
"""
from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, HTTPException

from . import config
from .k8s import KubernetesClient
from .migrator import run_migration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("migrator.api")

app = FastAPI(
    title="node-maintenance-policy storage-version migrator",
    version="1.0.0",
    description=(
        "把集群级自定义资源 NodeMaintenancePolicy 残留的 v1beta1 存储对象，"
        "通过 v1 API 读-写回，让 apiserver 按固定存储版本 v1 重新落盘。"
    ),
)

# 同一进程内不允许并发迁移（真实部署通常单副本 + leader 选举）
_migration_lock = asyncio.Lock()
_last_report: dict | None = None


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "kubernetesApi": config.KUBERNETES_API_URL}


@app.post("/migrate")
async def migrate() -> dict:
    global _last_report
    if _migration_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="another migration is in progress; wait for it to finish",
        )
    async with _migration_lock:
        async with KubernetesClient() as client:
            report = await run_migration(client)
    _last_report = report
    # 业务失败仍返回 200 + status=failed（报告里带出错处），便于运维脚本解析；
    # 仅前置条件/遍历类问题在报告 error 中说明，不抛 5xx。
    return report


@app.get("/migrate/last")
async def last_migration() -> dict:
    if _last_report is None:
        raise HTTPException(status_code=404, detail="no migration has run yet")
    return _last_report
