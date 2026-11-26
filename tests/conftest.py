"""测试夹具：用 ASGI in-process 方式同时挂载替身与迁移服务。"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app import config
from app.k8s import KubernetesClient
from app.main import app as migrator_app
from app.stub.app import app as stub_app
from app.stub.store import store

# 与 docker-compose.yml 保持一致，3 个种子对象会产生 2 页
config.PAGE_LIMIT = 2
config.RETRY_BACKOFF_SECONDS = 0.01


@pytest.fixture(autouse=True)
def _reset_store():
    store.reset()
    yield
    store.reset()


@pytest.fixture
async def stub_client():
    transport = ASGITransport(app=stub_app)
    async with AsyncClient(transport=transport, base_url="http://stub") as c:
        yield c


@pytest.fixture
async def client(monkeypatch):
    """迁移客户端：httpx 请求直接打到替身 ASGI app（不占端口）。"""
    transport = ASGITransport(app=stub_app)
    real_init = KubernetesClient.__init__

    def patched_init(self, base_url: str | None = None) -> None:
        real_init(self, base_url or "http://stub")
        self._client = AsyncClient(  # type: ignore[attr-defined]
            transport=transport,
            base_url=self.base_url,
            timeout=10,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    monkeypatch.setattr(KubernetesClient, "__init__", patched_init)

    transport_m = ASGITransport(app=migrator_app)
    async with AsyncClient(transport=transport_m, base_url="http://migrator") as c:
        yield c
