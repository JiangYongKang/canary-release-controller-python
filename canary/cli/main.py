"""Command-line client over the same durable local state as the HTTP service.

The CLI and the resident service coordinate through one state file plus a
flock sidecar, so they may run as separate processes (and scripts may
restart/retry) while observing exactly the same releases, policy versions,
decisions and history.

State file resolution (first match):
  1. --state PATH
  2. CANARY_STATE environment variable
  3. ./canary-state.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from canary.errors import CanaryError
from canary.models import ObservationPoint, ReleaseStatus
from canary.service import CanaryService

DEFAULT_STATE_PATH = "canary-state.json"


def _state_path(args: argparse.Namespace) -> str:
    return args.state or os.environ.get("CANARY_STATE") or DEFAULT_STATE_PATH


def _service(args: argparse.Namespace) -> CanaryService:
    return CanaryService(persist_path=_state_path(args))


def _emit(obj) -> None:
    json.dump(obj, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="canary", description=__doc__)
    parser.add_argument("--state", help="path to the shared local state file")
    sub = parser.add_subparsers(dest="command", required=True)

    # policies
    p_policy_create = sub.add_parser("policy-create", help="create a new immutable policy version")
    p_policy_create.add_argument("--name", default="default")
    p_policy_create.add_argument("--steps", required=True, help="comma-separated percents, e.g. 10,30,50,100")
    p_policy_create.add_argument("--threshold", type=float, default=0.05, dest="failure_rate_threshold")
    p_policy_create.add_argument("--min-requests", type=int, default=100)
    p_policy_create.add_argument("--observe-minutes", type=int, default=5)

    sub.add_parser("policy-list", help="list all policy versions")

    # releases
    p_register = sub.add_parser("register", help="register a canary release")
    p_register.add_argument("--service", required=True)
    p_register.add_argument("--environment", required=True)
    p_register.add_argument("--old", dest="old_version", required=True)
    p_register.add_argument("--new", dest="new_version", required=True)
    p_register.add_argument("--policy-version", type=int, default=None)

    p_get = sub.add_parser("get", help="show one release")
    p_get.add_argument("release_id")

    p_promote = sub.add_parser("promote", help="advance / evaluate a release")
    p_promote.add_argument("release_id")
    p_promote.add_argument("--step-index", type=int, default=None)
    p_promote.add_argument("--request-id", default="")

    p_rollback = sub.add_parser("rollback", help="roll a release back")
    p_rollback.add_argument("release_id")
    p_rollback.add_argument("--reason", default="manual")
    p_rollback.add_argument("--request-id", default="")

    p_observe = sub.add_parser("observe", help="record one observation point")
    p_observe.add_argument("release_id")
    p_observe.add_argument("--step", dest="step_index", type=int, required=True)
    p_observe.add_argument("--minute", dest="minute_offset", type=int, required=True)
    p_observe.add_argument("--total", dest="total_requests", type=int, required=True)
    p_observe.add_argument("--failed", dest="failed_requests", type=int, required=True)

    p_eval = sub.add_parser("evaluate", help="judge the current window without mutating state")
    p_eval.add_argument("release_id")

    p_history = sub.add_parser("history", help="list immutable conclusions")
    p_history.add_argument("--service", default=None)

    args = parser.parse_args(argv)
    try:
        return _dispatch(args)
    except CanaryError as exc:
        _emit({"error": {"code": exc.code, "message": exc.message, "details": exc.details}})
        return 1


def _dispatch(args: argparse.Namespace) -> int:
    svc = _service(args)
    cmd = args.command

    if cmd == "policy-create":
        steps = [int(x) for x in args.steps.split(",") if x.strip()]
        policy = svc.create_policy(
            args.name,
            steps,
            failure_rate_threshold=args.failure_rate_threshold,
            min_requests=args.min_requests,
            observe_minutes=args.observe_minutes,
        )
        _emit(policy.to_dict())
        return 0

    if cmd == "policy-list":
        _emit({"policies": [p.to_dict() for p in svc.policies.all_versions()]})
        return 0

    if cmd == "register":
        release = svc.register(
            service=args.service,
            environment=args.environment,
            old_version=args.old_version,
            new_version=args.new_version,
            policy_version=args.policy_version,
        )
        _emit(_release_dict(release))
        return 0

    if cmd == "get":
        _emit(_release_dict(svc.get_release(args.release_id)))
        return 0

    if cmd == "promote":
        result = svc.promote(
            args.release_id,
            step_index=args.step_index,
            request_id=args.request_id,
        )
        _emit({
            "release_id": result.release_id,
            "from_status": result.from_status.value,
            "to_status": result.to_status.value,
            "step_index": result.step_index,
            "new_version_percent": result.new_version_percent,
            "evaluated": result.evaluated,
            "rolled_back": result.rolled_back,
            "reason": result.reason,
        })
        return 0

    if cmd == "rollback":
        result = svc.rollback(
            args.release_id,
            reason=args.reason,
            request_id=args.request_id,
        )
        _emit({
            "release_id": result.release_id,
            "from_status": result.from_status.value,
            "to_status": result.to_status.value,
            "trigger": result.trigger,
            "reason": result.reason,
            "idempotent_hit": result.idempotent_hit,
        })
        return 0

    if cmd == "observe":
        svc.observe(
            args.release_id,
            args.step_index,
            ObservationPoint(
                minute_offset=args.minute_offset,
                total_requests=args.total_requests,
                failed_requests=args.failed_requests,
            ),
        )
        _emit(_release_dict(svc.get_release(args.release_id)))
        return 0

    if cmd == "evaluate":
        healthy, reason = svc.evaluate(args.release_id)
        _emit({"healthy": healthy, "reason": reason})
        return 0

    if cmd == "history":
        _emit({
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
                for h in svc.history(args.service)
            ]
        })
        return 0

    return 2


def _release_dict(r) -> dict:
    return {
        "release_id": r.release_id,
        "service": r.service,
        "environment": r.environment,
        "old_version": r.old_version,
        "new_version": r.new_version,
        "status": r.status.value,
        "policy_version": r.policy_version,
        "steps": [
            {"new_version_percent": s.new_version_percent, "observe_minutes": s.observe_minutes}
            for s in r.steps
        ],
        "current_step_index": r.current_step_index,
        "current_new_version_percent": (
            r.steps[r.current_step_index].new_version_percent
            if r.current_step_index >= 0
            else 0
        ),
        "finish_reason": r.finish_reason,
        "observations": {
            str(k): [
                {
                    "minute_offset": p.minute_offset,
                    "total_requests": p.total_requests,
                    "failed_requests": p.failed_requests,
                }
                for p in v
            ]
            for k, v in r.observations.items()
        },
        "created_at": r.created_at,
        "updated_at": r.updated_at,
    }


if __name__ == "__main__":
    raise SystemExit(main())
