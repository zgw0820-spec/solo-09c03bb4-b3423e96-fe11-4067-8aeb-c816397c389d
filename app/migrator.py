"""存储版本改存核心流程。

流程严格对齐 Kubernetes 官方 storage version migrator 的思路：
1. 校验 CRD 存在、集群级、唯一 storage=true 的版本是 v1，且 storedVersions 含 v1beta1；
   同时记录本轮开始时 CRD 的 UID；
2. 用 v1 分页 LIST 全部对象（continue 翻页直到无 next token）；
3. 对每个对象：GET 到的 v1 表示原样 PUT 回去（只换 resourceVersion 驱动重写），
   服务端按当前存储版本 v1 重新落盘 —— 不修改 spec/status 的任何内容；
4. 409 冲突 => 重新 GET 最新对象再试；404 => 对象已删除，跳过且不补建；
5. 只有遍历完整、且所有未删除对象都成功改存后，才把 v1beta1 从
   CRD status.storedVersions 移除；任一步失败且不可继续 => 保留旧标记并报错。

CRD 撤标的安全约束（只针对本轮开始时的那个 CRD）：
- PUT /status 携带最新 GET 到的完整 status，只改 storedVersions，conditions 等
  其他状态字段原样保留，绝不用只有 storedVersions 的 body 整体覆盖；
- 409 => 丢弃旧快照，重读最新 CRD 重新判断，不覆盖其他控制器刚写入的状态；
- 重读发现 v1beta1 已被别处撤掉 => 报告 already-absent，不再写状态；
- 重读发现 UID 变了（同名 CRD 被重建）=> 报 failed 且不写新 CRD 的任何状态。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass, field

from . import config
from .k8s import ApiError, KubernetesClient

logger = logging.getLogger("migrator.engine")


@dataclass
class ObjectResult:
    name: str
    outcome: str  # rewritten | skipped-deleted | failed
    attempts: int = 0
    conflictRetries: int = 0
    detail: str = ""


@dataclass
class MigrationReport:
    status: str  # succeeded | failed
    crd: str = config.CRD_NAME
    storageVersion: str = config.STORAGE_VERSION
    legacyVersionRemoved: bool = False
    # 旧标记的最终处置：removed（本轮成功撤标）| already-absent（别处已撤）|
    # retained（出错保留，可重跑）
    legacyVersionOutcome: str = "retained"
    crdUidChanged: bool = False
    storedVersionsBefore: list[str] = field(default_factory=list)
    storedVersionsAfter: list[str] = field(default_factory=list)
    pagesProcessed: int = 0
    objectsListed: int = 0
    objectsRewritten: int = 0
    objectsSkippedDeleted: int = 0
    objectsFailed: int = 0
    results: list[dict] = field(default_factory=list)
    error: dict | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class PreconditionError(Exception):
    """CRD 前置条件不满足，迁移不应继续。"""


def _check_crd_preconditions(crd: dict) -> None:
    if crd.get("kind") != "CustomResourceDefinition":
        raise PreconditionError(f"{config.CRD_NAME} 返回对象不是 CustomResourceDefinition")
    spec = crd.get("spec", {})
    if spec.get("scope") != "Cluster":
        raise PreconditionError(
            f"{config.CRD_NAME} scope={spec.get('scope')!r}，本服务只处理集群级自定义资源"
        )
    if spec.get("group") != config.CRD_GROUP:
        raise PreconditionError(f"{config.CRD_NAME} group 不匹配: {spec.get('group')!r}")
    storage_versions = [
        v["name"] for v in spec.get("versions", []) if v.get("storage")
    ]
    if storage_versions != [config.STORAGE_VERSION]:
        raise PreconditionError(
            f"迁移期间存储版本必须固定为 {config.STORAGE_VERSION}，"
            f"实际 storage=true 的版本为 {storage_versions}，拒绝迁移"
        )


async def _rewrite_one(
    client: KubernetesClient, name: str, report: MigrationReport
) -> ObjectResult:
    """GET 最新 v1 对象并原样 PUT 回去；409 重读重试，404 跳过。"""
    result = ObjectResult(name=name, outcome="failed")
    try:
        obj = await client.get_policy(name)
        result.attempts = 1
        while True:
            try:
                await client.put_policy(obj)
                result.outcome = "rewritten"
                report.objectsRewritten += 1
                logger.info("object %s rewritten as %s", name, config.STORAGE_VERSION)
                return result
            except ApiError as exc:
                if exc.status == 404:
                    # 遍历期间被删除：跳过，绝不补建
                    result.outcome = "skipped-deleted"
                    result.detail = "object deleted during migration; skipped, not recreated"
                    report.objectsSkippedDeleted += 1
                    logger.info("object %s deleted during migration, skipped", name)
                    return result
                if exc.status == 409:
                    if result.conflictRetries >= config.MAX_CONFLICT_RETRIES:
                        result.detail = (
                            f"conflict retries exhausted ({config.MAX_CONFLICT_RETRIES}): {exc.message}"
                        )
                        report.objectsFailed += 1
                        return result
                    result.conflictRetries += 1
                    result.attempts += 1
                    logger.warning(
                        "conflict on %s, re-read latest object and retry (%d/%d)",
                        name, result.conflictRetries, config.MAX_CONFLICT_RETRIES,
                    )
                    await asyncio.sleep(config.RETRY_BACKOFF_SECONDS * result.conflictRetries)
                    # 冲突 => 读最新对象再试（基于全新 resourceVersion）
                    obj = await client.get_policy(name)
                    continue
                # 4xx（非 404/409）无法通过重试解决；5xx 已在客户端层重试耗尽
                result.detail = exc.message
                report.objectsFailed += 1
                return result
    except ApiError as exc:
        if exc.status == 404:
            result.outcome = "skipped-deleted"
            result.detail = "object already gone when first read; skipped"
            report.objectsSkippedDeleted += 1
            return result
        result.detail = exc.message
        report.objectsFailed += 1
        return result


async def run_migration(client: KubernetesClient | None = None) -> dict:
    """执行一次完整改存，返回结构化报告。"""
    own_client = client is None
    client = client or KubernetesClient()
    report = MigrationReport(status="failed")
    try:
        # ------------------------------------------------ 1. 前置校验
        try:
            crd = await client.get_crd()
        except ApiError as exc:
            report.error = {"stage": "get-crd", "message": exc.message, "status": exc.status}
            return report.to_dict()
        try:
            _check_crd_preconditions(crd)
        except PreconditionError as exc:
            report.error = {"stage": "precondition", "message": str(exc)}
            return report.to_dict()

        before = list(crd.get("status", {}).get("storedVersions", []))
        report.storedVersionsBefore = before
        # 本轮开始时那个 CRD 的身份。后续任何状态写回都必须确认 UID 未变：
        # 若改存期间同名 CRD 被删除并重建，得到的是一个全新对象，绝不能写它。
        original_uid = crd.get("metadata", {}).get("uid")
        if config.LEGACY_VERSION not in before:
            # 幂等：没有旧标记时仍可重跑（把残留 v1beta1 编码对象改存），但无需再改 CRD
            logger.info("legacy version %s absent from storedVersions: %s",
                        config.LEGACY_VERSION, before)

        # ------------------------------------------------ 2. 分页遍历
        names_in_order: list[str] = []
        cont: str | None = None
        try:
            while True:
                page = await client.list_policies(limit=config.PAGE_LIMIT, cont=cont)
                report.pagesProcessed += 1
                items = page.get("items", [])
                for obj in items:
                    name = obj["metadata"]["name"]
                    names_in_order.append(name)
                report.objectsListed += len(items)
                cont = page.get("continue")
                if not cont:
                    break
        except ApiError as exc:
            # 分页失败且重试耗尽 => 无法保证遍历完整，保留 storedVersions 旧标记
            report.error = {
                "stage": "list",
                "message": exc.message,
                "status": exc.status,
                "atPage": report.pagesProcessed + 1,
                "nextContinueToken": cont,
                "hint": "遍历未完成，已保留 status.storedVersions 旧标记，修复后可重跑",
            }
            report.results = [
                asdict(ObjectResult(name=n, outcome="pending")) for n in names_in_order
            ]
            return report.to_dict()

        # ------------------------------------------------ 3. 逐对象改存
        for name in names_in_order:
            obj_result = await _rewrite_one(client, name, report)
            report.results.append(asdict(obj_result))

        if report.objectsFailed:
            failed = [r["name"] for r in report.results if r["outcome"] == "failed"]
            report.error = {
                "stage": "rewrite",
                "message": f"{len(failed)} 个对象写回失败，无法保证全部按 v1 落盘",
                "failedObjects": failed,
                "hint": "已保留 status.storedVersions 中的 v1beta1，修复后可安全重跑",
            }
            return report.to_dict()

        # -------------------------------- 4. 收敛 CRD status.storedVersions
        # 只在本轮开始时旧标记确实存在时才动 CRD；撤标原则：
        #  - 只从“本轮开始时那个 CRD”的 status.storedVersions 里删 v1beta1，
        #    其他版本与 status 的其余字段（conditions 等）全部原样保留；
        #  - 409 => 重读最新 CRD 重新判断，不拿旧快照覆盖其他控制器刚写入的状态；
        #  - 重读发现 v1beta1 已被别处撤掉 => 报告标记已不存在，不再写状态；
        #  - 重读发现 UID 变了（同名 CRD 被重建）=> 报失败，绝不写新 CRD。
        if config.LEGACY_VERSION in before:
            crd_attempts = 0
            while True:
                try:
                    latest_crd = await client.get_crd()
                except ApiError as exc:
                    report.error = {
                        "stage": "update-crd-status",
                        "message": f"重读最新 CRD 失败: {exc.message}",
                        "status": exc.status,
                        "hint": "对象改存已完成，旧标记仍保留，重跑本接口即可安全重试",
                    }
                    report.storedVersionsAfter = before
                    return report.to_dict()

                latest_meta = latest_crd.get("metadata", {})
                if latest_meta.get("uid") != original_uid:
                    # 同名 CRD 被删除后重建：新对象不是本轮改存对应的那个，
                    # 不写它的任何状态，按失败上报（对象改存结果仍在报告中）。
                    report.crdUidChanged = True
                    report.error = {
                        "stage": "update-crd-status",
                        "message": (
                            f"CRD {config.CRD_NAME} 的 UID 已变化"
                            f"（开始时 {original_uid!r}，现在 {latest_meta.get('uid')!r}），"
                            "疑似改存期间同名 CRD 被删除并重建；拒绝写入新 CRD 的状态"
                        ),
                        "hint": "请人工确认新 CRD 的 storedVersions 与对象落盘情况后再决定是否重跑",
                    }
                    report.storedVersionsAfter = list(
                        latest_crd.get("status", {}).get("storedVersions", [])
                    )
                    return report.to_dict()

                latest_versions = list(
                    latest_crd.get("status", {}).get("storedVersions", [])
                )
                if config.LEGACY_VERSION not in latest_versions:
                    # 旧标记已被别的组件撤掉：不重复写状态，按事实报告。
                    report.legacyVersionRemoved = False
                    report.legacyVersionOutcome = "already-absent"
                    report.storedVersionsAfter = latest_versions
                    logger.info(
                        "legacy version %s already removed from storedVersions by another "
                        "component; no CRD status write performed",
                        config.LEGACY_VERSION,
                    )
                    break

                # 仅从最新 storedVersions 中剔除 v1beta1，其余版本保持原顺序原样保留；
                # status 的其他字段（conditions 等）随 latest_crd 完整回写。
                desired = [v for v in latest_versions if v != config.LEGACY_VERSION]
                if config.STORAGE_VERSION not in desired:
                    desired.append(config.STORAGE_VERSION)
                latest_crd["status"]["storedVersions"] = desired
                try:
                    updated = await client.put_crd_status(latest_crd)
                except ApiError as exc:
                    if exc.status == 409 and crd_attempts < config.MAX_CONFLICT_RETRIES:
                        # CRD status 也被其他组件改动：丢弃本次基于旧 resourceVersion
                        # 的构造，下一轮重读最新 CRD 再重新判断（UID / 标记是否还在）。
                        crd_attempts += 1
                        logger.warning(
                            "conflict updating CRD status, re-read and re-evaluate "
                            "retry %d/%d",
                            crd_attempts, config.MAX_CONFLICT_RETRIES,
                        )
                        await asyncio.sleep(
                            config.RETRY_BACKOFF_SECONDS * crd_attempts
                        )
                        continue
                    # 对象已全部改存，只是 CRD 状态没收敛：保留旧标记，报告可重跑
                    report.error = {
                        "stage": "update-crd-status",
                        "message": exc.message,
                        "status": exc.status,
                        "hint": "对象改存已完成，重跑本接口即可安全重试 storedVersions 收敛",
                    }
                    report.storedVersionsAfter = before
                    return report.to_dict()
                report.legacyVersionRemoved = True
                report.legacyVersionOutcome = "removed"
                report.storedVersionsAfter = list(
                    updated["status"]["storedVersions"]
                )
                break
        else:
            report.legacyVersionOutcome = "already-absent"
            report.storedVersionsAfter = before

        report.status = "succeeded"
        return report.to_dict()
    finally:
        if own_client:
            await client.aclose()
