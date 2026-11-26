"""面向本任务的最小 Kubernetes API 客户端（httpx 实现）。

只依赖 CRD/CR 的标准 REST 端点，不使用 client-go 风格的 SDK；
所有 CR 访问固定走 v1 路径。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from . import config

logger = logging.getLogger("migrator.k8s")


class ApiError(Exception):
    def __init__(self, status: int, message: str, url: str):
        super().__init__(f"HTTP {status} for {url}: {message}")
        self.status = status
        self.message = message
        self.url = url


class KubernetesClient:
    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or config.KUBERNETES_API_URL).rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=config.HTTP_TIMEOUT_SECONDS,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "KubernetesClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ 工具
    async def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        url = f"{self.base_url}{path}"
        try:
            resp = await self._client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            # 网络层错误按可重试的 5xx 类处理
            raise ApiError(599, f"transport error: {exc!r}", url) from exc
        if resp.status_code >= 400:
            try:
                message = resp.json().get("message", resp.text)
            except ValueError:
                message = resp.text
            raise ApiError(resp.status_code, message, url)
        return resp.json()

    async def _request_with_retry(
        self, method: str, path: str, *, retry_on_5xx: bool = True, **kwargs: Any
    ) -> dict:
        attempt = 0
        while True:
            try:
                return await self._request(method, path, **kwargs)
            except ApiError as exc:
                server_failure = exc.status >= 500 or exc.status == 599
                if retry_on_5xx and server_failure and attempt < config.MAX_SERVER_RETRIES:
                    attempt += 1
                    delay = config.RETRY_BACKOFF_SECONDS * attempt
                    logger.warning(
                        "server error %s on %s, retry %d/%d in %.1fs",
                        exc.status, path, attempt, config.MAX_SERVER_RETRIES, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

    # ------------------------------------------------------------------- CRD
    async def get_crd(self) -> dict:
        path = f"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{config.CRD_NAME}"
        return await self._request_with_retry("GET", path)

    async def put_crd_status(self, crd: dict) -> dict:
        path = (
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/"
            f"{config.CRD_NAME}/status"
        )
        # 状态子资源写回：apiserver 会用 body.status 整体替换 CRD status，
        # 因此必须携带最新 GET 到的完整 status（含 conditions、acceptedNames
        # 等），只允许 storedVersions 与最新版本不同，其余字段原样回写，
        # 绝不能只发 storedVersions —— 那会清掉其他控制器维护的状态字段。
        body = {
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": config.CRD_NAME, "resourceVersion": crd["metadata"]["resourceVersion"]},
            "status": crd["status"],
        }
        return await self._request_with_retry("PUT", path, json=body)

    # -------------------------------------------------------------------- CR
    def _collection_path(self) -> str:
        # 固定 v1：迁移期间只通过 v1 读写
        return f"/apis/{config.CRD_GROUP}/{config.STORAGE_VERSION}/{config.CRD_PLURAL}"

    async def list_policies(self, limit: int, cont: str | None = None) -> dict:
        params: dict[str, Any] = {"limit": limit}
        if cont:
            params["continue"] = cont
        return await self._request_with_retry(
            "GET", self._collection_path(), params=params
        )

    async def get_policy(self, name: str) -> dict:
        return await self._request_with_retry(
            "GET", f"{self._collection_path()}/{name}"
        )

    async def put_policy(self, obj: dict) -> dict:
        name = obj["metadata"]["name"]
        # 原样写回 GET 到的 v1 对象（含其 resourceVersion），不增删任何字段
        return await self._request_with_retry(
            "PUT", f"{self._collection_path()}/{name}", json=obj, retry_on_5xx=True
        )
