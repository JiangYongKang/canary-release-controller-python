#!/usr/bin/env python3
"""Multi-process sharing + cross-restart continuation demo.

Run:  uv run python scripts/multiprocess_demo.py

Unlike demo.py (HTTP inside one process), this script spawns *separate OS
processes* for every action - simulating a resident service, a CLI and a
background script that share one state file, plus a restart mid-release.
It needs no setup: a temp directory is created and cleaned up automatically.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
WORKER = ROOT / "tests" / "_mp_worker.py"


def call(state: str, command: str, args: dict | None = None) -> dict:
    proc = subprocess.run(
        [sys.executable, str(WORKER), state, command, json.dumps(args or {})],
        capture_output=True, text=True, timeout=30, cwd=str(ROOT),
    )
    assert proc.returncode == 0, proc.stderr
    line = next(ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT:"))
    return json.loads(line[len("RESULT:"):])


def banner(t: str) -> None:
    print(f"\n=== {t} ===")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        state = str(Path(tmp) / "state.json")

        banner("1. process A creates a custom policy + starts a release")
        policy = call(state, "create_policy", {
            "name": "relaxed", "steps": [20, 60, 100],
            "failure_rate_threshold": 0.30, "min_requests": 250,
            "observe_minutes": 3,
        })
        rid = call(state, "register", {
            "service": "checkout", "environment": "prod",
            "old_version": "v1", "new_version": "v2",
            "policy_version": policy["version"],
        })["release_id"]
        call(state, "promote", {"release_id": rid,
                                "kwargs": {"request_id": "enter"}})

        banner("2. process B (fresh) sees A's policies and progress")
        print("policy versions visible to B:", call(state, "list_policies")["versions"])
        seen = call(state, "get_release", {"release_id": rid})
        print("B sees:", seen["status"], "step", seen["current_step_index"],
              "@", seen["current_percent"], "%")

        banner("3. B records a partial window; a NEW process 'restarts' and judges")
        call(state, "observe", {"release_id": rid, "step_index": 0,
                                "minute_offset": 0, "total_requests": 200,
                                "failed_requests": 0})
        pending = call(state, "evaluate", {"release_id": rid})
        print("restarted judgement (incomplete):", pending.get("code"),
              pending.get("details"))
        call(state, "observe", {"release_id": rid, "step_index": 0,
                                "minute_offset": 1, "total_requests": 0,
                                "failed_requests": 0})
        call(state, "observe", {"release_id": rid, "step_index": 0,
                                "minute_offset": 2, "total_requests": 50,
                                "failed_requests": 0})
        advanced = call(state, "promote",
                        {"release_id": rid,
                         "kwargs": {"request_id": "after-restart"}})
        print("continued after restart ->", advanced["to_status"],
              "step", advanced["step_index"], "@",
              advanced["new_version_percent"], "%")

        banner("4. cross-process idempotent replay of the same request id")
        replay = call(state, "promote",
                      {"release_id": rid,
                       "kwargs": {"request_id": "after-restart"}})
        print("replay ->", replay["to_status"], "step", replay["step_index"],
              "(no double advance)")

        banner("5. 16 fresh processes roll back with the same request id")
        procs = [
            subprocess.Popen(
                [sys.executable, str(WORKER), state, "rollback",
                 json.dumps({"release_id": rid,
                             "kwargs": {"reason": "manual",
                                        "request_id": "the-rb"}})],
                stdout=subprocess.DEVNULL, cwd=str(ROOT),
            )
            for _ in range(16)
        ]
        for p in procs:
            p.wait(timeout=30)
        final = call(state, "get_release", {"release_id": rid})
        history = call(state, "history", {})["entries"]
        print("final status:", final["status"])
        print("history entries for the release:",
              sum(1 for h in history if h["release_id"] == rid))

    print("\nMULTI-PROCESS DEMO DONE")


if __name__ == "__main__":
    main()
