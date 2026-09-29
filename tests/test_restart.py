"""Cross-restart continuation & on-disk compatibility tests.

A "restart" here is a brand-new ``CanaryService`` process opened on the same
state file: nothing survives in memory, only the file does.  These tests pin
that unfinished releases (including ones using a custom non-default policy)
continue with exactly the same thresholds / sample requirements / observation
durations, that finished conclusions never change after restart or repeated
evaluation, and that old/partial files keep stable meaning.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests._mp_worker_test_support import run_worker

WORKER = str(Path(__file__).with_name("_mp_worker.py"))


@pytest.fixture
def state_path(tmp_path: Path) -> str:
    return str(tmp_path / "canary-state.json")


def test_unfinished_release_continues_after_restart_with_custom_policy(state_path):
    print("\nSCENARIO: mid-ramp release under a custom policy survives restart")
    created = run_worker(state_path, "create_policy", {
        "name": "relaxed-long", "steps": [20, 60, 100],
        "failure_rate_threshold": 0.3, "min_requests": 250, "observe_minutes": 3,
    })
    assert created["version"] == 2
    reg = run_worker(state_path, "register", {
        "service": "billing", "environment": "prod",
        "old_version": "v1", "new_version": "v2", "policy_version": 2,
    })
    rid = reg["release_id"]
    run_worker(state_path, "promote",
               {"release_id": rid, "kwargs": {"request_id": "enter"}})
    # feed only 2 of 3 minutes and too few samples, then "restart"
    for minute in range(2):
        run_worker(state_path, "observe", {
            "release_id": rid, "step_index": 0,
            "minute_offset": minute, "total_requests": 100, "failed_requests": 0,
        })

    # ---- restart: a completely fresh process reads only the file ----
    rel = run_worker(state_path, "get_release", {"release_id": rid})
    assert rel["status"] == "canary" and rel["current_step_index"] == 0
    assert rel["policy_version"] == 2
    assert rel["steps"] == [20, 60, 100]
    assert rel["observe_minutes"] == [3, 3, 3]
    assert rel["observations"] == {"0": 2}
    print("RESTART: release still at step0 with 2/3 minutes and custom ramp")

    # same thresholds govern the resumed judgement: window still incomplete
    pending = run_worker(state_path, "evaluate", {"release_id": rid})
    assert pending["ok"] is False and pending["code"] == "observation_pending"
    assert pending["details"]["required_minutes"] == 3
    assert pending["details"]["min_requests"] == 250
    print("RESTART: judgement still requires 3 minutes and >=250 samples")

    # finish the window *after restart*: total 250 samples, 0 failures -> healthy
    run_worker(state_path, "observe", {
        "release_id": rid, "step_index": 0,
        "minute_offset": 2, "total_requests": 50, "failed_requests": 0,
    })
    adv = run_worker(state_path, "promote",
                     {"release_id": rid, "kwargs": {"request_id": "after-restart"}})
    assert adv["to_status"] == "canary" and adv["step_index"] == 1
    assert adv["new_version_percent"] == 60
    print("RESTART: release advances to step1 @60% under the same custom policy")

    # boundary meaning unchanged: fail_rate 75/250 == 0.30 threshold -> rollback
    for minute in range(3):
        run_worker(state_path, "observe", {
            "release_id": rid, "step_index": 1,
            "minute_offset": minute,
            "total_requests": 250 if minute == 0 else 0,
            "failed_requests": 75 if minute == 0 else 0,
        })
    rb = run_worker(state_path, "promote",
                    {"release_id": rid, "kwargs": {"request_id": "judge2"}})
    assert rb["rolled_back"] is True and rb["to_status"] == "rolled_back"
    print("RESTART: threshold 0.30 still boundary-inclusive after restart -> rb")


def test_finished_conclusion_is_immutable_across_restart_and_reevaluation(state_path):
    print("\nSCENARIO: completed release never changes after restart/reevaluation")
    reg = run_worker(state_path, "register", {
        "service": "svc", "environment": "prod",
        "old_version": "v1", "new_version": "v2",
    })
    rid = reg["release_id"]
    run_worker(state_path, "promote",
               {"release_id": rid, "kwargs": {"request_id": "e"}})
    for step in range(4):
        for minute in range(5):
            run_worker(state_path, "observe", {
                "release_id": rid, "step_index": step,
                "minute_offset": minute, "total_requests": 100, "failed_requests": 0,
            })
        done = run_worker(state_path, "promote", {
            "release_id": rid, "kwargs": {"request_id": f"p{step}"}})
    assert done["to_status"] == "completed"

    # restart once, twice; replay old request ids; inspect history each time
    for restart in range(2):
        rel = run_worker(state_path, "get_release", {"release_id": rid})
        assert rel["status"] == "completed" and rel["finish_reason"]
        replay = run_worker(state_path, "promote", {
            "release_id": rid, "kwargs": {"request_id": f"p3"}})
        assert replay["to_status"] == "completed" and replay["rolled_back"] is False
        hist = run_worker(state_path, "history", {})["entries"]
        mine = [h for h in hist if h["release_id"] == rid]
        assert len(mine) == 1 and mine[0]["final_status"] == "completed"
        assert mine[0]["policy_snapshot"]["failure_rate_threshold"] == 0.05
        print(f"RESTART {restart + 1}: still COMPLETED, single history, replay ok")


def test_legacy_state_file_without_policies_section_keeps_stable_defaults(
    state_path,
):
    print("\nSCENARIO: old-format file (no policies section, sparse fields)")
    legacy = {
        "releases": [{
            "release_id": "rel-old",
            "service": "legacy-svc", "environment": "prod",
            "old_version": "v1", "new_version": "v2",
            "status": "pending",
            "policy_version": 1,
            "steps": [{"new_version_percent": 10}, {"new_version_percent": 100}],
            "current_step_index": -1,
            # created_at / updated_at / finish_reason / observations intentionally
            # omitted to simulate a very old writer
        }],
        "decisions": [],
        "history": [{
            "release_id": "rel-old2",
            "service": "legacy-svc", "environment": "staging",
            "old_version": "v0", "new_version": "v1",
            "final_status": "completed",
            # reason / policy_snapshot / finished_at omitted
        }],
    }
    with open(state_path, "w", encoding="utf-8") as fh:
        json.dump(legacy, fh)

    rel = run_worker(state_path, "get_release", {"release_id": "rel-old"})
    assert rel["status"] == "pending" and rel["observe_minutes"] == [5, 5]
    print("LEGACY: missing observe_minutes -> 5; missing scalar fields -> defaults")

    # default policy v1 is transparently restored so the old release can proceed
    versions = run_worker(state_path, "list_policies")["versions"]
    assert versions == [1]
    enter = run_worker(state_path, "promote",
                       {"release_id": "rel-old", "kwargs": {"request_id": "e"}})
    assert enter["new_version_percent"] == 10
    hist = run_worker(state_path, "history", {})["entries"]
    old2 = [h for h in hist if h["release_id"] == "rel-old2"][0]
    assert old2["final_status"] == "completed" and old2["reason"] == ""
    assert old2["policy_snapshot"] == {}
    print("LEGACY: sparse history entry loads with empty reason/snapshot defaults")


def test_state_file_is_never_torn_by_concurrent_process_writers(state_path):
    print("\nSCENARIO: hammering writes from many processes never corrupts file")
    procs = []
    for i in range(8):
        args = json.dumps({
            "name": f"p{i}", "steps": [10 * (j + 1) if j < 9 else 100
                                       for j in range(10)],
            "failure_rate_threshold": 0.1, "min_requests": 1, "observe_minutes": 1,
        })
        procs.append(subprocess.Popen(
            [sys.executable, WORKER, state_path, "create_policy", args],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ))
    for p in procs:
        out, err = p.communicate(timeout=30)
        assert p.returncode == 0, err
    # file must be valid JSON with a gap-free version sequence and no leftovers
    with open(state_path, encoding="utf-8") as fh:
        payload = json.load(fh)
    versions = sorted(p["version"] for p in payload["policies"])
    assert versions == list(range(1, 10)), versions
    leftovers = [n for n in os.listdir(os.path.dirname(state_path))
                 if n.startswith(".canary-state.")]
    assert leftovers == [], leftovers
    print(f"8 concurrent policy creators -> gap-free {versions}; no temp files")


def test_killed_process_during_commit_leaves_consistent_state(tmp_path):
    print("\nSCENARIO: process killed mid-commit leaves no half-written state")
    state_path = str(tmp_path / "killed.json")
    # A long-running worker holding the cross-process lock is terminated hard;
    # we simulate this with a tiny inline subprocess that enters a transaction
    # and sleeps while holding it, then SIGKILL it.
    holder = r'''
import os, sys, time, fcntl
sys.path.insert(0, %r)
from canary.store import LocalStore
store = LocalStore(persist_path=%r)
with store.lock():
    store.put_policy({"version": 1, "name": "default",
                      "failure_rate_threshold": 0.05, "min_requests": 100,
                      "created_at": "",
                      "steps": [{"new_version_percent": 100,
                                 "observe_minutes": 5}]})
    time.sleep(30)
''' % (os.getcwd(), state_path)
    proc = subprocess.Popen([sys.executable, "-c", holder])
    time.sleep(1.0)
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)

    # The killed tx never committed: either no file or the pre-existing file;
    # in both cases a fresh service opens cleanly and stays usable.
    svc_proc = subprocess.run(
        [sys.executable, WORKER, state_path, "ping", "{}"],
        capture_output=True, text=True, timeout=30,
    )
    assert svc_proc.returncode == 0, svc_proc.stderr
    assert "RESULT:" in svc_proc.stdout
    # ensure_default seeds v1 after the killed transaction's uncommitted write
    versions = run_worker(state_path, "list_policies")["versions"]
    assert versions == [1]
    print("KILL: fresh process boots; killed uncommitted tx left no torn file")


def test_corrupt_state_file_raises_without_deadlocking(tmp_path):
    print("\nSCENARIO: corrupt state file surfaces an error, file lock not leaked")
    state_path = str(tmp_path / "corrupt.json")
    with open(state_path, "w", encoding="utf-8") as fh:
        fh.write("{not valid json")
    # a fresh process fails loudly rather than silently starting blank
    import subprocess
    bad = subprocess.run(
        [sys.executable, WORKER, state_path, "ping", "{}"],
        capture_output=True, text=True, timeout=30,
    )
    assert bad.returncode != 0
    assert "RESULT:" not in bad.stdout
    # repair the file; a new process must acquire the very same flock and run,
    # proving the failed process did not leak the cross-process lock
    with open(state_path, "w", encoding="utf-8") as fh:
        json.dump({"releases": [], "decisions": [], "history": []}, fh)
    ok = run_worker(state_path, "list_policies")
    assert ok["versions"] == [1]  # default policy seeded on open
    print("CORRUPT: error surfaced; repaired file opens, flock re-acquirable")
