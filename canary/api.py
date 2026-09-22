"""HTTP 入口（FastAPI）：与 CLI/测试共享同一个 ReleaseService。

非法/越级/终态重复操作统一转换为 409，并在 body 中给出可区分的 code 与 reason。
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from canary.errors import CanaryError
from canary.models import Release, release_to_dict
from canary.service import ReleaseService


# ---------------- 请求/响应模型 ----------------
class RegisterRequest(BaseModel):
    release_id: str = Field(..., min_length=1)
    service: str = Field(..., min_length=1)
    environment: str = Field(..., min_length=1)
    old_version: str = Field(..., min_length=1)
    new_version: str = Field(..., min_length=1)
    policy_name: str = "default"
    policy_version: int | None = None
    idempotency_key: str | None = None


class PromoteRequest(BaseModel):
    target_stage: int | None = None
    idempotency_key: str | None = None


class RollbackRequest(BaseModel):
    reason: str = "manual"
    idempotency_key: str | None = None


class MetricRequest(BaseModel):
    service: str = Field(..., min_length=1)
    environment: str = Field(..., min_length=1)
    success: bool
    ts: float | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class PolicyVersionRequest(BaseModel):
    definition: dict[str, Any]
    note: str | None = None


def _release_view(release: Release) -> dict[str, Any]:
    data = release_to_dict(release)
    data["current_weight"] = release.current_weight
    return data


def create_app(service: ReleaseService) -> FastAPI:
    """构建 FastAPI 应用并注入服务实例。"""
    app = FastAPI(title="canary-release-controller", version="0.1.0")

    @app.exception_handler(CanaryError)
    async def _on_canary_error(_request: Any, exc: CanaryError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": exc.code.value, "reason": exc.reason},
        )

    # ---------------- 策略 ----------------
    @app.get("/policies")
    def list_policies() -> dict[str, Any]:
        return {"policies": service._store.list_policies()}  # noqa: SLF001

    @app.post("/policies/{name}/versions")
    def register_policy_version(name: str, body: PolicyVersionRequest) -> dict[str, Any]:
        version = service.register_policy_version(name, body.definition)
        return {"policy": name, "version": version}

    @app.get("/policies/{name}/versions/{version}")
    def get_policy_version(name: str, version: int) -> dict[str, Any]:
        return {"policy": name, "version": version, "definition": service.get_policy_version(name, version)}

    # ---------------- 发布单 ----------------
    @app.post("/releases")
    def register_release(body: RegisterRequest) -> dict[str, Any]:
        release = service.register_release(
            release_id=body.release_id,
            service=body.service,
            environment=body.environment,
            old_version=body.old_version,
            new_version=body.new_version,
            policy_name=body.policy_name,
            policy_version=body.policy_version,
            idempotency_key=body.idempotency_key,
        )
        return _release_view(release)

    @app.get("/releases")
    def list_releases() -> dict[str, Any]:
        return {"releases": [_release_view(r) for r in service._store.list_releases()]}  # noqa: SLF001

    @app.get("/releases/{release_id}")
    def get_release(release_id: str) -> dict[str, Any]:
        return _release_view(service.get_release(release_id))

    @app.post("/releases/{release_id}/promote")
    def promote(release_id: str, body: PromoteRequest) -> dict[str, Any]:
        release = service.promote(
            release_id,
            target_stage=body.target_stage,
            idempotency_key=body.idempotency_key,
        )
        return _release_view(release)

    @app.post("/releases/{release_id}/rollback")
    def rollback(release_id: str, body: RollbackRequest) -> dict[str, Any]:
        release = service.rollback(
            release_id,
            reason=body.reason,
            idempotency_key=body.idempotency_key,
        )
        return _release_view(release)

    @app.get("/releases/{release_id}/health")
    def evaluate_health(release_id: str) -> dict[str, Any]:
        release, verdict = service.evaluate_health(release_id)
        return {"release": _release_view(release), "verdict": asdict(verdict)}

    @app.post("/releases/{release_id}/auto-check")
    def auto_check(release_id: str) -> dict[str, Any]:
        release, verdict = service.auto_check(release_id)
        return {"release": _release_view(release), "verdict": asdict(verdict)}

    @app.get("/releases/{release_id}/history")
    def reevaluate_history(release_id: str) -> dict[str, Any]:
        return service.reevaluate_history(release_id)

    # ---------------- 观测信号 ----------------
    @app.post("/metrics")
    def record_metric(body: MetricRequest) -> dict[str, Any]:
        sample = service._metrics.record(  # noqa: SLF001
            body.service,
            body.environment,
            body.success,
            ts=body.ts,
            detail=body.detail,
        )
        return {"recorded": True, "ts": sample.ts, "success": sample.success}

    return app
