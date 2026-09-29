"""Subprocess worker for cross-process tests.

Invoked as ``python tests/_mp_worker.py <state_path> <command> <json-args>``.
Each invocation constructs a *fresh* ``CanaryService`` (its own process, its own
memory), performs one command against the shared state file and prints a single
JSON result line prefixed with ``RESULT:``.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from canary.errors import CanaryError  # noqa: E402
from canary.models import ObservationPoint  # noqa: E402
from canary.service import CanaryService  # noqa: E402


def _emit(payload: dict) -> None:
    print("RESULT:" + json.dumps(payload, ensure_ascii=False, default=str))


def main() -> int:
    state_path = sys.argv[1]
    command = sys.argv[2]
    args = json.loads(sys.argv[3]) if len(sys.argv) > 3 else {}
    svc = CanaryService(persist_path=state_path)
    try:
        if command == "ping":
            _emit({"ok": True})
        elif command == "create_policy":
            policy = svc.create_policy(
                args["name"],
                args["steps"],
                failure_rate_threshold=args.get("failure_rate_threshold", 0.05),
                min_requests=args.get("min_requests", 100),
                observe_minutes=args.get("observe_minutes", 5),
            )
            _emit({"ok": True, "version": policy.version})
        elif command == "register":
            release = svc.register(**args)
            _emit({"ok": True, "release_id": release.release_id,
                   "policy_version": release.policy_version})
        elif command == "observe":
            svc.observe(
                args["release_id"],
                args["step_index"],
                ObservationPoint(
                    minute_offset=args["minute_offset"],
                    total_requests=args["total_requests"],
                    failed_requests=args["failed_requests"],
                ),
            )
            _emit({"ok": True})
        elif command == "promote":
            result = svc.promote(args["release_id"], **args.get("kwargs", {}))
            _emit({"ok": True, "to_status": result.to_status.value,
                   "step_index": result.step_index,
                   "new_version_percent": result.new_version_percent,
                   "rolled_back": result.rolled_back, "reason": result.reason})
        elif command == "rollback":
            result = svc.rollback(args["release_id"], **args.get("kwargs", {}))
            _emit({"ok": True, "to_status": result.to_status.value,
                   "trigger": result.trigger, "reason": result.reason})
        elif command == "get_release":
            rel = svc.get_release(args["release_id"])
            _emit({
                "ok": True,
                "status": rel.status.value,
                "policy_version": rel.policy_version,
                "current_step_index": rel.current_step_index,
                "current_percent": (
                    rel.steps[rel.current_step_index].new_version_percent
                    if rel.current_step_index >= 0 else 0
                ),
                "steps": [s.new_version_percent for s in rel.steps],
                "observe_minutes": [s.observe_minutes for s in rel.steps],
                "observations": {
                    str(k): len(v) for k, v in rel.observations.items()
                },
                "finish_reason": rel.finish_reason,
            })
        elif command == "evaluate":
            healthy, reason = svc.evaluate(args["release_id"])
            _emit({"ok": True, "healthy": healthy, "reason": reason})
        elif command == "list_policies":
            _emit({"ok": True, "versions": [p.version for p in svc.policies.all_versions()]})
        elif command == "history":
            entries = svc.history(args.get("service"))
            _emit({"ok": True, "entries": [
                {"release_id": h.release_id,
                 "final_status": h.final_status.value,
                 "reason": h.reason,
                 "policy_snapshot": h.policy_snapshot}
                for h in entries
            ]})
        else:
            _emit({"ok": False, "error": f"unknown command {command}"})
    except CanaryError as exc:
        _emit({"ok": False, "code": exc.code, "details": exc.details})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
