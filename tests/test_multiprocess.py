"""Cross-process sharing & concurrency tests.

Every worker is a genuinely separate OS process (fresh interpreter, fresh
in-memory caches) attached to the same JSON state file.  These tests assert
that the long-running service, the CLI and background scripts behave like one
coherent system: writes are immediately visible to other entry points,
concurrent decisions serialize to one conclusion, and retries stay idempotent
across processes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tests._mp_worker_test_support import run_worker


@pytest.fixture
def state_path(tmp_path: Path) -> str:
    return str(tmp_path / "canary-state.json")


def test_separate_entry_points_share_release_state_policy_and_history(state_path):
    print("\nSCENARIO: service + script processes share one local state file")
    # process 1 ("service") creates a custom policy and starts a release
    p2 = run_worker(state_path, "create_policy", {
        "name": "custom", "steps": [20, 60, 100],
        "failure_rate_threshold": 0.2, "min_requests": 10, "observe_minutes": 1,
    })
    assert p2["ok"] and p2["version"] == 2
    reg = run_worker(state_path, "register", {
        "service": "checkout", "environment": "prod",
        "old_version": "v1", "new_version": "v2", "policy_version": 2,
    })
    rid = reg["release_id"]
    run_worker(state_path, "promote",
               {"release_id": rid, "kwargs": {"request_id": "enter"}})

    # process 2 ("CLI script"): must see policy set, release and its progress
    assert run_worker(state_path, "list_policies")["versions"] == [1, 2]
    seen = run_worker(state_path, "get_release", {"release_id": rid})
    assert seen["status"] == "canary" and seen["current_step_index"] == 0
    assert seen["current_percent"] == 20
    print("SCRIPT sees service's release at step0 @20% and policies [1, 2]")

    # script feeds the window and advances the release on its own
    run_worker(state_path, "observe", {
        "release_id": rid, "step_index": 0,
        "minute_offset": 0, "total_requests": 100, "failed_requests": 0,
    })
    adv = run_worker(state_path, "promote",
                     {"release_id": rid, "kwargs": {"request_id": "cli-adv"}})
    assert adv["to_status"] == "canary" and adv["step_index"] == 1
    assert adv["new_version_percent"] == 60

    # process 3 ("background script") immediately sees the CLI's commit
    later = run_worker(state_path, "get_release", {"release_id": rid})
    assert later["current_step_index"] == 1 and later["current_percent"] == 60
    assert later["observations"] == {"0": 1}
    print("BACKGROUND process sees CLI's observation + advance to step1 @60%")

    # idempotency ledger is shared too: a third process replays the CLI request
    replay = run_worker(state_path, "promote",
                        {"release_id": rid, "kwargs": {"request_id": "cli-adv"}})
    assert replay["step_index"] == 1 and replay["rolled_back"] is False
    print("request_id 'cli-adv' replayed in yet another process -> same result")


def _start_unhealthy_release(state_path) -> str:
    """Fresh processes: custom policy, register, enter step0, unhealthy window."""
    run_worker(state_path, "create_policy", {
        "name": "p", "steps": [10, 50, 100],
        "failure_rate_threshold": 0.05, "min_requests": 100, "observe_minutes": 2,
    })
    reg = run_worker(state_path, "register", {
        "service": "svc", "environment": "prod",
        "old_version": "v1", "new_version": "v2", "policy_version": 2,
    })
    rid = reg["release_id"]
    run_worker(state_path, "promote",
               {"release_id": rid, "kwargs": {"request_id": "enter"}})
    for minute in range(2):
        run_worker(state_path, "observe", {
            "release_id": rid, "step_index": 0,
            "minute_offset": minute, "total_requests": 100, "failed_requests": 10,
        })  # 10% >= 5% threshold -> unhealthy
    return rid


def _spawn_many(state_path, specs: list[tuple[str, dict]], workers: int = 16):
    """Launch all specs in separate processes at once; collect result payloads."""
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(run_worker, state_path, cmd, args)
                for cmd, args in specs]
        return [f.result() for f in futs]


def test_cross_process_same_request_id_single_rollback(state_path):
    rid = _start_unhealthy_release(state_path)
    print("\nSCENARIO: 12 processes fire the SAME rollback request_id at once")
    specs = [
        ("rollback", {"release_id": rid,
                      "kwargs": {"reason": "manual", "request_id": "the-rb"}})
        for _ in range(12)
    ]
    results = _spawn_many(state_path, specs)
    # all calls report the same terminal conclusion; ledger makes it idempotent
    assert all(r["ok"] and r["to_status"] == "rolled_back" for r in results)
    hist = run_worker(state_path, "history", {})["entries"]
    mine = [h for h in hist if h["release_id"] == rid]
    assert len(mine) == 1, mine  # no duplicate conclusions across processes
    final = run_worker(state_path, "get_release", {"release_id": rid})
    assert final["status"] == "rolled_back"
    print(f"all 12 processes agree; history entries for release = {len(mine)}")


def test_cross_process_mixed_promote_and_rollback_one_conclusion(state_path):
    rid = _start_unhealthy_release(state_path)
    print("\nSCENARIO: 8 promote + 8 rollback processes, distinct request ids")
    specs = [("promote", {"release_id": rid, "kwargs": {"request_id": f"p{i}"}})
             for i in range(8)]
    specs += [("rollback", {"release_id": rid,
                            "kwargs": {"reason": "manual", "request_id": f"r{i}"}})
              for i in range(8)]
    results = _spawn_many(state_path, specs)
    winners = [r for r in results if r["ok"]]
    losers = [r for r in results if not r["ok"]]
    # exactly one call produces the rolled_back conclusion; everyone else loses
    assert len(winners) == 1, results
    win = winners[0]
    assert win["to_status"] == "rolled_back"
    # a winning "promote" reports the auto-rollback; a winning "rollback" too
    if "rolled_back" in win:
        assert win["rolled_back"] is True
    assert all(r["code"] == "release_finished" for r in losers), losers
    hist = run_worker(state_path, "history", {})["entries"]
    mine = [h for h in hist if h["release_id"] == rid]
    assert len(mine) == 1 and mine[0]["final_status"] == "rolled_back"
    final = run_worker(state_path, "get_release", {"release_id": rid})
    assert final["status"] == "rolled_back"
    print(f"one winner ({win.get('reason', win.get('trigger'))}), "
          f"{len(losers)} release_finished, one history entry")


def test_cross_process_promotes_climb_one_step_at_a_time(state_path):
    # healthy window at step 0; many distinct promote requests from processes
    # must advance at most one step (the rest find the new window pending).
    run_worker(state_path, "create_policy", {
        "name": "p", "steps": [10, 50, 100],
        "failure_rate_threshold": 0.05, "min_requests": 100, "observe_minutes": 2,
    })
    reg = run_worker(state_path, "register", {
        "service": "svc", "environment": "prod",
        "old_version": "v1", "new_version": "v2", "policy_version": 2,
    })
    rid = reg["release_id"]
    run_worker(state_path, "promote",
               {"release_id": rid, "kwargs": {"request_id": "enter"}})
    for minute in range(2):
        run_worker(state_path, "observe", {
            "release_id": rid, "step_index": 0,
            "minute_offset": minute, "total_requests": 100, "failed_requests": 0,
        })
    print("\nSCENARIO: 10 distinct promote processes on a healthy step0")
    specs = [("promote", {"release_id": rid, "kwargs": {"request_id": f"m{i}"}})
             for i in range(10)]
    results = _spawn_many(state_path, specs, workers=10)
    winners = [r for r in results if r["ok"]]
    losers = [r for r in results if not r["ok"]]
    assert len(winners) == 1, results
    assert winners[0]["step_index"] == 1 and winners[0]["new_version_percent"] == 50
    assert all(r["code"] == "observation_pending" for r in losers), losers
    final = run_worker(state_path, "get_release", {"release_id": rid})
    assert final["current_step_index"] == 1  # never jumped two steps
    assert run_worker(state_path, "history", {})["entries"] == []
    print(f"exactly one advance step0->step1; {len(losers)} pending; no history")


# ---------------------------------------------------------------------- #
# CLI entry point (each call is an independent process)
# ---------------------------------------------------------------------- #
def _run_cli(state_path: str, *cli_args: str) -> tuple[int, dict]:
    proc = subprocess.run(
        [sys.executable, "-m", "canary.cli", "--state", state_path, *cli_args],
        capture_output=True, text=True, timeout=30,
    )
    return proc.returncode, json.loads(proc.stdout.strip().splitlines()[-1])


def test_cli_processes_share_state_with_engine_process(state_path):
    print("\nSCENARIO: CLI process and worker (service) process interleave")
    code, out = _run_cli(state_path, "create-policy", "--name", "x",
                         "--steps", "50,100", "--threshold", "0.2",
                         "--min-requests", "10", "--observe-minutes", "1")
    assert code == 0 and out["version"] == 2
    reg = run_worker(state_path, "register", {
        "service": "cli-svc", "environment": "prod",
        "old_version": "v1", "new_version": "v2", "policy_version": 2,
    })
    rid = reg["release_id"]
    code, out = _run_cli(state_path, "promote", rid, "--request-id", "e")
    assert code == 0 and out["to_status"] == "canary"
    # service-side process sees what the CLI process did
    seen = run_worker(state_path, "get_release", {"release_id": rid})
    assert seen["current_percent"] == 50 and seen["status"] == "canary"
    # CLI error path: duplicate promote of the same request replays; bad step 429
    code, out = _run_cli(state_path, "promote", rid, "--step", "9",
                         "--request-id", "bad")
    assert code == 1 and out["error"]["code"] == "step_skipping"
    print("CLI<->service interleaving works; invalid CLI call -> exit 1 + code")


def test_long_lived_process_observes_commits_from_other_process(state_path, tmp_path):
    print("\nSCENARIO: one long-lived process keeps working while another commits")
    # A long-lived service, simulated by a small script that performs an action,
    # waits for a marker file, then performs a second action in a NEW transaction.
    holder = tmp_path / "holder.py"
    holder.write_text(
        "import sys, time\n"
        "from canary.service import CanaryService\n"
        "svc = CanaryService(persist_path=sys.argv[1])\n"
        "rid = sys.argv[2]; marker = sys.argv[3]\n"
        "svc.promote(rid, request_id='enter')           # tx 1\n"
        "while not __import__('os').path.exists(marker):\n"
        "    time.sleep(0.05)\n"
        "rel = svc.get_release(rid)                      # tx 2: must reload\n"
        "n = len(rel.observations.get(0, []))\n"
        "print('POINTS', n)\n"
        "assert n == 2, 'resident process read a stale cache'\n"
    )
    marker = tmp_path / "go"
    reg = run_worker(state_path, "register", {
        "service": "resident", "environment": "prod",
        "old_version": "v1", "new_version": "v2",
    })
    rid = reg["release_id"]
    env = dict(os.environ)
    repo_root = str(Path(__file__).resolve().parent.parent)
    env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, str(holder), state_path, rid, str(marker)],
        stdout=subprocess.PIPE, text=True, env=env,
    )
    # wait until the resident process has entered step 0
    for _ in range(50):
        seen = run_worker(state_path, "get_release", {"release_id": rid})
        if seen["status"] == "canary":
            break
        time.sleep(0.05)
    # a different process writes two observations while A is idle
    for minute in range(2):
        run_worker(state_path, "observe", {
            "release_id": rid, "step_index": 0,
            "minute_offset": minute, "total_requests": 100, "failed_requests": 0,
        })
    marker.write_text("go")
    out, _ = proc.communicate(timeout=20)
    assert proc.returncode == 0, out
    assert "POINTS 2" in out
    print("resident process's next transaction saw the other process's 2 points")
