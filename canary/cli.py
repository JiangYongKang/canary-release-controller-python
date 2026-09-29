"""Command-line entry point operating on the same shared local state.

Usage example (state file shared with a long-running HTTP service)::

    python -m canary.cli --state ./canary-state.json register \
        --service checkout --environment prod --old v1 --new v2

Every invocation is an independent process: it opens the state file, runs one
command, and exits.  Cross-process locking makes it safe to run alongside the
resident service or other scripts, and every invocation sees the latest
committed state.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Sequence

from canary.errors import CanaryError
from canary.models import ObservationPoint
from canary.service import CanaryService

DEFAULT_STATE_PATH = os.environ.get(
    "CANARY_STATE_PATH", os.path.join(os.getcwd(), "canary-state.json")
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="canary", description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE_PATH,
                        help="path to the shared local state JSON file")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list-policies", help="list all policy versions")

    p_create = sub.add_parser("create-policy", help="create an immutable policy")
    p_create.add_argument("--name", default="default")
    p_create.add_argument("--steps", required=True,
                          help="comma-separated percents, e.g. 10,30,50,100")
    p_create.add_argument("--threshold", type=float, default=0.05)
    p_create.add_argument("--min-requests", type=int, default=100)
    p_create.add_argument("--observe-minutes", type=int, default=5)

    p_reg = sub.add_parser("register", help="register a release")
    p_reg.add_argument("--service", required=True)
    p_reg.add_argument("--environment", required=True)
    p_reg.add_argument("--old", dest="old_version", required=True)
    p_reg.add_argument("--new", dest="new_version", required=True)
    p_reg.add_argument("--policy-version", type=int, default=None)

    p_promote = sub.add_parser("promote", help="advance / complete a release")
    p_promote.add_argument("release_id")
    p_promote.add_argument("--step", dest="step_index", type=int, default=None)
    p_promote.add_argument("--request-id", default="")

    p_rollback = sub.add_parser("rollback", help="roll a release back")
    p_rollback.add_argument("release_id")
    p_rollback.add_argument("--reason", default="manual")
    p_rollback.add_argument("--request-id", default="")

    p_obs = sub.add_parser("observe", help="record one observation point")
    p_obs.add_argument("release_id")
    p_obs.add_argument("--step", dest="step_index", type=int, required=True)
    p_obs.add_argument("--minute", dest="minute_offset", type=int, required=True)
    p_obs.add_argument("--total", dest="total_requests", type=int, required=True)
    p_obs.add_argument("--failed", dest="failed_requests", type=int, required=True)

    p_get = sub.add_parser("status", help="show one release")
    p_get.add_argument("release_id")

    p_eval = sub.add_parser("evaluate", help="judge the current window only")
    p_eval.add_argument("release_id")

    sub.add_parser("history", help="list immutable history conclusions")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    svc = CanaryService(persist_path=args.state)
    try:
        result = _dispatch(svc, args)
    except CanaryError as exc:
        print(json.dumps({"error": {"code": exc.code,
                                    "message": exc.message,
                                    "details": exc.details}},
                         ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


def _release_view(release) -> dict:
    return {
        "release_id": release.release_id,
        "service": release.service,
        "environment": release.environment,
        "old_version": release.old_version,
        "new_version": release.new_version,
        "status": release.status.value,
        "policy_version": release.policy_version,
        "current_step_index": release.current_step_index,
        "current_new_version_percent": (
            release.steps[release.current_step_index].new_version_percent
            if release.current_step_index >= 0 else 0
        ),
        "steps": [
            {"new_version_percent": s.new_version_percent,
             "observe_minutes": s.observe_minutes}
            for s in release.steps
        ],
        "finish_reason": release.finish_reason,
        "updated_at": release.updated_at,
    }


def _dispatch(svc: CanaryService, args) -> dict:
    command = args.command
    if command == "list-policies":
        return {"policies": [p.to_dict() for p in svc.policies.all_versions()]}
    if command == "create-policy":
        steps = [int(x) for x in args.steps.split(",") if x.strip()]
        policy = svc.create_policy(
            args.name, steps,
            failure_rate_threshold=args.threshold,
            min_requests=args.min_requests,
            observe_minutes=args.observe_minutes,
        )
        return policy.to_dict()
    if command == "register":
        release = svc.register(
            service=args.service, environment=args.environment,
            old_version=args.old_version, new_version=args.new_version,
            policy_version=args.policy_version,
        )
        return _release_view(release)
    if command == "promote":
        result = svc.promote(
            args.release_id, step_index=args.step_index,
            request_id=args.request_id,
        )
        return {
            "from_status": result.from_status.value,
            "to_status": result.to_status.value,
            "step_index": result.step_index,
            "new_version_percent": result.new_version_percent,
            "evaluated": result.evaluated,
            "rolled_back": result.rolled_back,
            "reason": result.reason,
            "release": _release_view(svc.get_release(args.release_id)),
        }
    if command == "rollback":
        result = svc.rollback(
            args.release_id, reason=args.reason, request_id=args.request_id,
        )
        return {
            "from_status": result.from_status.value,
            "to_status": result.to_status.value,
            "trigger": result.trigger,
            "reason": result.reason,
            "idempotent_hit": result.idempotent_hit,
            "release": _release_view(svc.get_release(args.release_id)),
        }
    if command == "observe":
        svc.observe(
            args.release_id, args.step_index,
            ObservationPoint(
                minute_offset=args.minute_offset,
                total_requests=args.total_requests,
                failed_requests=args.failed_requests,
            ),
        )
        return _release_view(svc.get_release(args.release_id))
    if command == "status":
        return _release_view(svc.get_release(args.release_id))
    if command == "evaluate":
        healthy, reason = svc.evaluate(args.release_id)
        return {"healthy": healthy, "reason": reason}
    if command == "history":
        return {"history": [
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
            for h in svc.history()
        ]}
    raise ValueError(f"unknown command {command}")  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
