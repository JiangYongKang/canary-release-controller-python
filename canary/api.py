"""FastAPI HTTP entry point.

All illegal operations return HTTP 4xx with a domain ``error.code`` so the
rejection reason remains distinguishable across entry points:

* release_not_found / policy_not_found -> 404
* release_already_exists / step_skipping -> 409
* release_finished / illegal_state -> 409
* observation_pending -> 425 (too early)
* invalid_argument -> 422
* unhealthy auto-rollback is *not* an error: the promote call returns the
  rolled-back conclusion in its body.
"""

from __future__ import annotations

from dataclasses import asdict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from canary.errors import CanaryError
from canary.models import ObservationPoint, ReleaseStatus
from canary.service import CanaryService

_HTTP_STATUS = {
    "release_not_found": 404,
    "policy_not_found": 404,
    "invalid_argument": 422,
    "invalid_policy": 422,
    "release_already_exists": 409,
    "release_finished": 409,
    "illegal_state": 409,
    "step_skipping": 409,
    "observation_pending": 425,
}


# ---------------------------------------------------------------------- #
# Request / response schemas
# ---------------------------------------------------------------------- #
class RegisterIn(BaseModel):
    service: str
    environment: str
    old_version: str
    new_version: str
    policy_version: int | None = None


class PolicyIn(BaseModel):
    name: str = "default"
    steps: list[int]
    failure_rate_threshold: float = 0.05
    min_requests: int = 100
    observe_minutes: int = 5


class PromoteIn(BaseModel):
    step_index: int | None = None
    request_id: str = ""


class RollbackIn(BaseModel):
    reason: str = "manual"
    request_id: str = ""


class ObserveIn(BaseModel):
    step_index: int
    minute_offset: int = Field(ge=0)
    total_requests: int = Field(ge=0)
    failed_requests: int = Field(ge=0)


def _release_view(r) -> dict:
    return {
        "release_id": r.release_id,
        "service": r.service,
        "environment": r.environment,
        "old_version": r.old_version,
        "new_version": r.new_version,
        "status": r.status.value,
        "policy_version": r.policy_version,
        "steps": [asdict(s) for s in r.steps],
        "current_step_index": r.current_step_index,
        "current_new_version_percent": (
            r.steps[r.current_step_index].new_version_percent
            if r.current_step_index >= 0
            else 0
        ),
        "finish_reason": r.finish_reason,
        "observations": {
            str(k): [asdict(p) for p in v] for k, v in r.observations.items()
        },
        "created_at": r.created_at,
        "updated_at": r.updated_at,
    }


def create_app(service: CanaryService | None = None) -> FastAPI:
    app = FastAPI(title="canary-release-controller")
    app.state.service = service or CanaryService()

    @app.exception_handler(CanaryError)
    async def handle_canary_error(_: Request, exc: CanaryError):
        return JSONResponse(
            status_code=_HTTP_STATUS.get(exc.code, 400),
            content={"error": {"code": exc.code, "message": exc.message, "details": exc.details}},
        )

    svc = app.state.service

    @app.post("/api/policies")
    def create_policy(body: PolicyIn):
        policy = svc.create_policy(
            body.name,
            body.steps,
            failure_rate_threshold=body.failure_rate_threshold,
            min_requests=body.min_requests,
            observe_minutes=body.observe_minutes,
        )
        return policy.to_dict()

    @app.get("/api/policies")
    def list_policies():
        return {"policies": [p.to_dict() for p in svc.policies.all_versions()]}

    @app.post("/api/releases")
    def register_release(body: RegisterIn):
        release = svc.register(**body.model_dump())
        return _release_view(release)

    @app.get("/api/releases/{release_id}")
    def get_release(release_id: str):
        return _release_view(svc.get_release(release_id))

    @app.post("/api/releases/{release_id}/observe")
    def observe(release_id: str, body: ObserveIn):
        point = ObservationPoint(
            minute_offset=body.minute_offset,
            total_requests=body.total_requests,
            failed_requests=body.failed_requests,
        )
        svc.observe(release_id, body.step_index, point)
        return _release_view(svc.get_release(release_id))

    @app.post("/api/releases/{release_id}/promote")
    def promote(release_id: str, body: PromoteIn):
        result = svc.promote(
            release_id, step_index=body.step_index, request_id=body.request_id
        )
        view = {
            "release_id": result.release_id,
            "from_status": result.from_status.value,
            "to_status": result.to_status.value,
            "step_index": result.step_index,
            "new_version_percent": result.new_version_percent,
            "evaluated": result.evaluated,
            "rolled_back": result.rolled_back,
            "reason": result.reason,
        }
        return {"result": view, "release": _release_view(svc.get_release(release_id))}

    @app.post("/api/releases/{release_id}/rollback")
    def rollback(release_id: str, body: RollbackIn):
        result = svc.rollback(
            release_id, reason=body.reason, request_id=body.request_id
        )
        view = {
            "release_id": result.release_id,
            "from_status": result.from_status.value,
            "to_status": result.to_status.value,
            "trigger": result.trigger,
            "reason": result.reason,
            "idempotent_hit": result.idempotent_hit,
        }
        return {"result": view, "release": _release_view(svc.get_release(release_id))}

    @app.get("/api/releases/{release_id}/evaluate")
    def evaluate(release_id: str):
        healthy, reason = svc.evaluate(release_id)
        return {"healthy": healthy, "reason": reason}

    @app.get("/api/history")
    def history(service: str | None = None):
        return {
            "history": [
                {
                    "release_id": h.release_id,
                    "service": h.service,
                    "environment": h.environment,
                    "old_version": h.old_version,
                    "new_version": h.new_version,
                    "final_status": h.final_status.value,
                    "reason": h.reason,
                    "policy_snapshot": h.policy_snapshot,
                    "finished_at": h.finished_at,
                }
                for h in svc.history(service)
            ]
        }

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = create_app()
