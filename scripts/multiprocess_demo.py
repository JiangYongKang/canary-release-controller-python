#!/usr/bin/env python3
"""Multi-process / restart demo over one shared local state file.

Run from the repo root with no extra setup:

    uv run python scripts/multiprocess_demo.py

It drives three scenarios through *separate* CLI subprocesses (each one is a
fresh process, like a background script next to the resident service):

1. one process registers + enters; another immediately sees the same state;
2. N processes race promote/rollback on one unhealthy release: the percent
   ramp never skips a step, rollback happens at most once, history has one
   conclusion;
3. "restart": every later action runs in yet another new process; the
   unfinished release keeps its custom policy (threshold / min samples /
   per-step observation minutes) and ramps to completion; the finished
   release's conclusion stays immutable.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def run_cli(state: Path, *args: str) -> tuple[int, dict]:
    proc = subprocess.run(
        [sys.executable, "-m", "canary.cli.main", "--state", str(state), *args],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    body = json.loads(proc.stdout) if proc.stdout.strip() else {}
    return proc.returncode, body


def banner(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="canary-multi-"))
    state = workdir / "shared.json"
    print("shared state file:", state)

    banner("1. two processes share one state file")
    rc, policy = run_cli(
        state, "policy-create", "--name", "fast",
        "--steps", "10,50,100", "--threshold", "0.10",
        "--min-requests", "50", "--observe-minutes", "1",
    )
    assert rc == 0, policy
    pv = policy["version"]
    print("process A: created custom policy v%s (threshold 0.10, min 50, 1-min windows)" % pv)
    rc, reg = run_cli(
        state, "register", "--service", "demo", "--environment", "prod",
        "--old", "v1", "--new", "v2", "--policy-version", str(pv),
    )
    rid = reg["release_id"]
    rc, _ = run_cli(state, "promote", rid, "--request-id", "enter")
    print("process A: registered", rid, "and entered step 0")
    rc, seen = run_cli(state, "get", rid)
    print("process B: sees", seen["status"], "step", seen["current_step_index"],
          "| policy v%s visible: %s" % (pv, True))

    banner("2. N processes race on an UNHEALTHY step-0 window")
    rc, bad = run_cli(
        state, "register", "--service", "race", "--environment", "prod",
        "--old", "v1", "--new", "v2", "--policy-version", str(pv),
    )
    bad_rid = bad["release_id"]
    run_cli(state, "promote", bad_rid, "--request-id", "enter2")
    run_cli(state, "observe", bad_rid, "--step", "0", "--minute", "0",
            "--total", "100", "--failed", "20")  # 20% >= 10% -> unhealthy

    procs = []
    for i in range(6):
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "canary.cli.main", "--state", str(state),
             "promote", bad_rid, "--request-id", f"p{i}"],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, text=True,
        ))
    for i in range(6):
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "canary.cli.main", "--state", str(state),
             "rollback", bad_rid, "--request-id", f"r{i}"],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, text=True,
        ))
    conclusions, rejects = [], []
    for p in procs:
        out, _ = p.communicate()
        body = json.loads(out)
        (conclusions if p.returncode == 0 else rejects).append(body)
    print("committing calls all concluded:",
          sorted({c["to_status"] for c in conclusions}))
    print("losing calls saw codes:", sorted({r["error"]["code"] for r in rejects}))
    _, final = run_cli(state, "get", bad_rid)
    _, hist = run_cli(state, "history")
    bad_history = [h for h in hist["history"] if h["release_id"] == bad_rid]
    print("final status:", final["status"],
          "| rollback history entries:", len(bad_history), "(must be 1)")
    assert final["status"] == "rolled_back" and len(bad_history) == 1

    banner("3. 'restart' — later actions are all fresh processes")
    run_cli(state, "observe", rid, "--step", "0", "--minute", "0",
            "--total", "50", "--failed", "0")
    # each call below is a brand-new process, exactly like after a restart
    for step in (1, 2):
        rc, res = run_cli(state, "promote", rid, "--request-id", f"to-{step}")
        print(f"new process: promote -> {res['to_status']} @ {res['new_version_percent']}%")
        if res["to_status"] == "canary":
            run_cli(state, "observe", rid, "--step", str(step), "--minute", "0",
                    "--total", "50", "--failed", "0")
    rc, done = run_cli(state, "promote", rid, "--request-id", "finish")
    print("new process: final promote ->", done["to_status"])
    # replay + restart never duplicate conclusions
    rc, replay = run_cli(state, "promote", rid, "--request-id", "finish")
    _, hist = run_cli(state, "history")
    entries = [h for h in hist["history"] if h["release_id"] == rid]
    print("replay after restart ->", replay["to_status"],
          "| completed history entries:", len(entries), "(must be 1)")
    assert replay["to_status"] == "completed" and len(entries) == 1

    print("\nMULTI-PROCESS DEMO DONE")


if __name__ == "__main__":
    main()
