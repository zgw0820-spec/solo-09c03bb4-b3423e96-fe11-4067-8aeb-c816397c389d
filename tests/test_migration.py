"""改存服务关键流程测试。"""
from __future__ import annotations

from app import config
from app.stub.store import V1BETA1, store


async def _storage(stub_client):
    r = await stub_client.get("/__control/storage")
    assert r.status_code == 200
    return r.json()


async def test_happy_path_all_v1beta1_rewritten_and_flag_removed(client, stub_client):
    # 前置：3 个对象全部以 v1beta1 编码，storedVersions 含 v1beta1
    view = await _storage(stub_client)
    assert {o["storageVersion"] for o in view["objects"]} == {V1BETA1}
    assert view["storedVersions"] == ["v1beta1", "v1"]

    r = await client.post("/migrate")
    assert r.status_code == 200
    report = r.json()
    assert report["status"] == "succeeded"
    assert report["pagesProcessed"] >= 2  # PAGE_LIMIT 见下，确保分页真的发生
    assert report["objectsListed"] == 3
    assert report["objectsRewritten"] == 3
    assert report["objectsSkippedDeleted"] == 0
    assert report["legacyVersionRemoved"] is True
    assert report["storedVersionsBefore"] == ["v1beta1", "v1"]
    assert report["storedVersionsAfter"] == ["v1"]

    view = await _storage(stub_client)
    assert {o["storageVersion"] for o in view["objects"]} == {"v1"}
    assert view["storedVersions"] == ["v1"]


async def test_spec_and_status_unchanged_after_rewrite(client, stub_client):
    before = (await _storage(stub_client))["objects"]
    r = await client.post("/migrate")
    assert r.json()["status"] == "succeeded"
    after = (await _storage(stub_client))["objects"]

    assert [o["name"] for o in before] == [o["name"] for o in after]
    for b, a in zip(before, after):
        assert b["spec"] == a["spec"], f"{b['name']} spec changed"
        assert b["status"] == a["status"], f"{b['name']} status changed"


async def test_idempotent_rerun(client, stub_client):
    first = (await client.post("/migrate")).json()
    assert first["status"] == "succeeded"

    second = (await client.post("/migrate")).json()
    assert second["status"] == "succeeded"
    assert second["legacyVersionRemoved"] is False  # 旧标记已不在，无需再删
    assert second["storedVersionsAfter"] == ["v1"]
    assert second["objectsRewritten"] == 3  # 对象仍会被无害地重新读-写回一次


async def test_conflict_then_reread_and_retry_succeeds(client, stub_client):
    # 第一次 PUT edge-nodes 时，替身模拟控制器并发修改 spec 并返回 409
    await stub_client.put("/__control/hooks", json={
        "concurrentEdit": {
            "name": "edge-nodes",
            "patch": {"maxUnavailable": 7},
        }
    })
    r = await client.post("/migrate")
    report = r.json()
    assert report["status"] == "succeeded", report
    edge = next(x for x in report["results"] if x["name"] == "edge-nodes")
    assert edge["outcome"] == "rewritten"
    assert edge["conflictRetries"] == 1
    assert edge["attempts"] == 2

    # 并发修改的 spec 必须保留（重读拿到的是最新对象，迁移不覆盖它）
    view = await _storage(stub_client)
    edge_now = next(o for o in view["objects"] if o["name"] == "edge-nodes")
    assert edge_now["spec"]["maxUnavailable"] == 7
    assert edge_now["storageVersion"] == "v1"


async def test_deleted_object_is_skipped_not_recreated(client, stub_client):
    # 首页（limit=2）之后、第二页之前，替身把第二页对象删掉（按名称排序第二页是 edge-nodes）
    await stub_client.put("/__control/hooks", json={
        "deleteOnFirstList": {"name": "edge-nodes"}
    })
    r = await client.post("/migrate")
    report = r.json()
    assert report["status"] == "succeeded", report
    # 删除发生在翻第二页之前，只有首页 2 个对象被列出并改存
    assert report["objectsListed"] == 2
    assert report["objectsSkippedDeleted"] == 0  # 没出现在列表里 => 根本不会被处理

    view = await _storage(stub_client)
    names = [o["name"] for o in view["objects"]]
    assert "edge-nodes" not in names
    assert view["storedVersions"] == ["v1"]


async def test_delete_during_rewrite_returns_404_and_skips(client, stub_client):
    # 列表完整返回 3 个，但改存 default-maintenance 时它已被删除（GET 即 404）
    real_get = store.get_object

    async def patched_get(name: str, version: str = "v1"):
        if name == "default-maintenance":
            from app.stub.store import ApiException
            raise ApiException(404, "NotFound", f'"{name}" not found')
        return await real_get(name, version)

    import app.stub.store as store_mod
    store_mod.store.get_object = patched_get  # type: ignore[method-assign]
    try:
        report = (await client.post("/migrate")).json()
    finally:
        store_mod.store.get_object = real_get  # type: ignore[method-assign]

    assert report["status"] == "succeeded", report
    result = next(x for x in report["results"] if x["name"] == "default-maintenance")
    assert result["outcome"] == "skipped-deleted"
    assert report["objectsSkippedDeleted"] == 1
    assert report["legacyVersionRemoved"] is True


async def test_transient_500_put_is_retried_and_succeeds(client, stub_client):
    await stub_client.put("/__control/hooks", json={
        "put500Once": {"name": "edge-nodes"}
    })
    report = (await client.post("/migrate")).json()
    assert report["status"] == "succeeded", report
    edge = next(x for x in report["results"] if x["name"] == "edge-nodes")
    assert edge["outcome"] == "rewritten"
    # 一次性 500 被 HTTP 客户端层的 5xx 重试吸收，引擎层面首次尝试即成功
    assert edge["attempts"] == 1
    assert edge["conflictRetries"] == 0


async def test_persistent_list_failure_keeps_legacy_flag(client, stub_client):
    await stub_client.put("/__control/hooks", json={
        "list500": {"status": 500, "reason": "InternalError",
                    "message": "etcd is unavailable", "repeat": True}
    })
    report = (await client.post("/migrate")).json()
    assert report["status"] == "failed"
    assert report["error"]["stage"] == "list"
    assert report["legacyVersionRemoved"] is False

    view = await _storage(stub_client)
    assert "v1beta1" in view["storedVersions"]  # 旧标记保留
    assert all(o["storageVersion"] == V1BETA1 for o in view["objects"])


async def test_persistent_put_failure_keeps_legacy_flag(client, stub_client):
    # 每次写 edge-nodes 都 500（持续故障），客户端重试耗尽后该对象失败
    await stub_client.put("/__control/hooks", json={
        "put500": {"name": "edge-nodes", "status": 500,
                   "reason": "InternalError", "message": "disk full",
                   "repeat": True}
    })
    report = (await client.post("/migrate")).json()
    assert report["status"] == "failed"
    assert report["error"]["stage"] == "rewrite"
    assert "edge-nodes" in report["error"]["failedObjects"]
    assert report["legacyVersionRemoved"] is False

    view = await _storage(stub_client)
    assert "v1beta1" in view["storedVersions"]
    edge = next(o for o in view["objects"] if o["name"] == "edge-nodes")
    assert edge["storageVersion"] == V1BETA1  # 失败对象未改存


async def test_rerun_after_failure_converges(client, stub_client):
    await stub_client.put("/__control/hooks", json={
        "put500": {"name": "edge-nodes", "status": 500,
                   "reason": "InternalError", "message": "disk full",
                   "repeat": True}
    })
    first = (await client.post("/migrate")).json()
    assert first["status"] == "failed"

    # 故障恢复后重跑 => 收敛
    await stub_client.put("/__control/hooks", json={})
    second = (await client.post("/migrate")).json()
    assert second["status"] == "succeeded", second
    assert second["storedVersionsAfter"] == ["v1"]
    view = await _storage(stub_client)
    assert {o["storageVersion"] for o in view["objects"]} == {"v1"}


async def test_precondition_rejects_wrong_storage_version(client, stub_client, monkeypatch):
    from app.k8s import KubernetesClient

    async def fake_get_crd(self):  # noqa: ANN001
        crd = await store.get_crd()
        crd["spec"]["versions"][0]["storage"] = False
        crd["spec"]["versions"][1]["storage"] = True  # v1beta1 被标成存储版本
        return crd

    monkeypatch.setattr(KubernetesClient, "get_crd", fake_get_crd)
    report = (await client.post("/migrate")).json()
    assert report["status"] == "failed"
    assert report["error"]["stage"] == "precondition"
    assert "v1" in report["error"]["message"]


async def test_seed_new_v1beta1_object_is_picked_up(client, stub_client):
    # 首轮收敛后，又出现一个历史写入路径产生的 v1beta1 对象
    await client.post("/migrate")
    body = {
        "apiVersion": f"{config.CRD_GROUP}/v1beta1",
        "kind": "NodeMaintenancePolicy",
        "metadata": {"name": "late-arrival", "generation": 1},
        "spec": {
            "drainTimeout": "90s",
            "maxUnavailable": 3,
            "nodeSelector": {"role": "batch"},
            "maintenanceWindow": "Mon 00:00-02:00",
        },
        "status": {"phase": "Pending"},
    }
    r = await stub_client.post("/__control/seed-v1beta1", json=body)
    assert r.status_code == 200

    report = (await client.post("/migrate")).json()
    assert report["status"] == "succeeded"
    late = next(x for x in report["results"] if x["name"] == "late-arrival")
    assert late["outcome"] == "rewritten"
    view = await _storage(stub_client)
    late_now = next(o for o in view["objects"] if o["name"] == "late-arrival")
    assert late_now["storageVersion"] == "v1"
    # 转换往返无损：90s 在 v1 里是整数秒
    assert late_now["spec"]["drainTimeoutSeconds"] == 90


async def test_v1beta1_to_v1_conversion_roundtrip(stub_client):
    # 直接经 v1 GET 验证服务端转换（drainTimeout 字符串 -> 秒）
    r = await stub_client.get("/apis/ops.example.com/v1/nodemaintenancepolicies/edge-nodes")
    obj = r.json()
    assert obj["apiVersion"] == "ops.example.com/v1"
    assert obj["spec"]["drainTimeoutSeconds"] == 120
    assert "drainTimeout" not in obj["spec"]

    # 同一对象经 v1beta1 GET 仍是老字段形状（status 原样保留）
    r2 = await stub_client.get(
        "/apis/ops.example.com/v1beta1/nodemaintenancepolicies/edge-nodes"
    )
    old = r2.json()
    assert old["apiVersion"] == "ops.example.com/v1beta1"
    assert old["spec"]["drainTimeout"] == "2m"
    assert "drainTimeoutSeconds" not in old["spec"]
    assert old["status"]["phase"] == "Draining"


async def test_pagination_visits_every_object_once(client, stub_client, monkeypatch):
    monkeypatch.setattr(config, "PAGE_LIMIT", 1)
    report = (await client.post("/migrate")).json()
    assert report["status"] == "succeeded"
    assert report["pagesProcessed"] == 3
    names = [r["name"] for r in report["results"]]
    assert sorted(names) == sorted({
        "default-maintenance", "edge-nodes", "database-keepers"
    })
    assert len(names) == len(set(names))


async def test_crd_status_conflict_is_retried_and_converges(client, stub_client, monkeypatch):
    from app.k8s import ApiError, KubernetesClient

    calls = {"n": 0}
    real_put = KubernetesClient.put_crd_status

    async def flaky_put(self, crd):  # noqa: ANN001
        calls["n"] += 1
        if calls["n"] == 1:
            raise ApiError(409, "Conflict: CRD status modified by another component", "crd/status")
        return await real_put(self, crd)

    monkeypatch.setattr(KubernetesClient, "put_crd_status", flaky_put)
    report = (await client.post("/migrate")).json()
    assert report["status"] == "succeeded", report
    assert calls["n"] == 2
    assert report["storedVersionsAfter"] == ["v1"]


async def test_concurrent_migrations_rejected(client):
    import asyncio

    import app.main as main_mod

    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_migration(_client):  # noqa: ANN001
        entered.set()
        await release.wait()
        return {"status": "succeeded", "results": []}

    original = main_mod.run_migration
    main_mod.run_migration = slow_migration  # type: ignore[assignment]
    try:
        first_task = asyncio.create_task(client.post("/migrate"))
        await entered.wait()
        second = await client.post("/migrate")
        assert second.status_code == 409
        release.set()
        first = await first_task
        assert first.status_code == 200
    finally:
        main_mod.run_migration = original  # type: ignore[assignment]
