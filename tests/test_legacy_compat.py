"""Legacy state-file format compatibility tests.

Business areas covered (each case prints AREA/CASE/RESULT lines so the test
log shows exactly which legacy scenario was exercised and how it ended):

1. missing-field records  — old writers omitted ``policy_version`` (and other
   fields); the whole file must still load, good records included.
2. dangling policy refs   — an unfinished release bound to a non-default
   policy whose definition was never persisted must keep evaluating and
   promoting under the stable default parameters.
3. restart x re-evaluation — the same old record, reopened in fresh processes
   any number of times, yields byte-identical judgements; finished releases
   never gain a new conclusion or history entry.

A "restart" is a brand-new OS process (``tests/_mp_worker.py`` or the real
CLI) opened on the same state file: nothing survives but the file itself.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tests._mp_worker_test_support import run_worker

WORKER = str(Path(__file__).with_name("_mp_worker.py"))
REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def state_path(tmp_path: Path) -> str:
    return str(tmp_path / "legacy-state.json")


def _write_state(state_path: str, payload: dict) -> None:
    with open(state_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)


def _run_cli(state_path: str, *argv: str) -> subprocess.CompletedProcess:
    """Invoke the real CLI as an independent process on the same file."""
    return subprocess.run(
        [sys.executable, "-m", "canary.cli", "--state", state_path, *argv],
        capture_output=True, text=True, timeout=30, cwd=REPO_ROOT, check=False,
    )


# --------------------------------------------------------------------- #
# AREA 1: records with missing fields
# --------------------------------------------------------------------- #
def test_missing_policy_version_field_loads_with_fixed_default(state_path):
    print("\nAREA 1: legacy records with missing fields")
    print("CASE: release record has no policy_version at all")
    _write_state(state_path, {
        "releases": [
            {   # the old record: no policy_version, no timestamps
                "release_id": "rel-legacy",
                "service": "legacy-svc", "environment": "prod",
                "old_version": "v1", "new_version": "v2",
                "status": "canary",
                "steps": [{"new_version_percent": 50},
                          {"new_version_percent": 100}],
                "current_step_index": 0,
            },
            {   # a well-formed record in the same file must survive too
                "release_id": "rel-good",
                "service": "good-svc", "environment": "prod",
                "old_version": "v1", "new_version": "v2",
                "status": "pending",
                "policy_version": 1,
                "steps": [{"new_version_percent": 100, "observe_minutes": 5}],
                "current_step_index": -1,
                "created_at": "t1", "updated_at": "t1",
            },
        ],
        "decisions": [],
        "history": [{
            "release_id": "rel-done", "service": "legacy-svc",
            "environment": "staging", "old_version": "v0",
            "new_version": "v1", "final_status": "completed",
        }],
    })

    # Opening the file must not crash; the old record gets fixed defaults.
    rel = run_worker(state_path, "get_release", {"release_id": "rel-legacy"})
    assert rel["policy_version"] == 1
    assert rel["status"] == "canary" and rel["current_step_index"] == 0
    assert rel["observe_minutes"] == [5, 5]
    print(f"RESULT: rel-legacy loaded, policy_version defaulted to "
          f"{rel['policy_version']}, observe_minutes={rel['observe_minutes']}")

    good = run_worker(state_path, "get_release", {"release_id": "rel-good"})
    assert good["status"] == "pending" and good["policy_version"] == 1
    hist = run_worker(state_path, "history", {})["entries"]
    assert [h["release_id"] for h in hist] == ["rel-done"]
    print("RESULT: sibling good release + history entry in same file intact")

    # The real CLI (another entry point) opens the same file without crashing.
    proc = _run_cli(state_path, "status", "rel-legacy")
    assert proc.returncode == 0, proc.stderr
    view = json.loads(proc.stdout)
    assert view["release_id"] == "rel-legacy" and view["policy_version"] == 1
    print("RESULT: CLI `status` on the old record exits 0 with same defaults")

    # Reopening any number of times yields the same view (fixed defaults).
    again = run_worker(state_path, "get_release", {"release_id": "rel-legacy"})
    assert again == rel
    print("RESULT: reopening gives an identical record (deterministic)")


def test_missing_policy_version_release_can_finish_under_defaults(state_path):
    print("\nAREA 1: legacy records with missing fields")
    print("CASE: field-less mid-ramp release promotes/completes via defaults")
    _write_state(state_path, {
        "releases": [{
            "release_id": "rel-legacy",
            "service": "legacy-svc", "environment": "prod",
            "old_version": "v1", "new_version": "v2",
            "status": "canary",
            "steps": [{"new_version_percent": 100, "observe_minutes": 2}],
            "current_step_index": 0,
        }],
        "decisions": [], "history": [],
    })
    # default min_requests=100, window of 2 minutes (from the stored step)
    for minute in range(2):
        run_worker(state_path, "observe", {
            "release_id": "rel-legacy", "step_index": 0,
            "minute_offset": minute, "total_requests": 60, "failed_requests": 1,
        })
    done = run_worker(state_path, "promote", {
        "release_id": "rel-legacy", "kwargs": {"request_id": "finish"}})
    assert done["to_status"] == "completed" and done["rolled_back"] is False
    hist = run_worker(state_path, "history", {})["entries"]
    assert len(hist) == 1 and hist[0]["final_status"] == "completed"
    snap = hist[0]["policy_snapshot"]
    assert snap["failure_rate_threshold"] == 0.05
    assert snap["min_requests"] == 100
    print(f"RESULT: completed under defaults; history snapshot "
          f"threshold={snap['failure_rate_threshold']} "
          f"min_requests={snap['min_requests']}")


# --------------------------------------------------------------------- #
# AREA 2: releases bound to a policy version that was never persisted
# --------------------------------------------------------------------- #
def test_dangling_policy_reference_uses_stable_default_parameters(state_path):
    print("\nAREA 2: release bound to an undefined (non-default) policy")
    print("CASE: policy_version=9 absent from file -> stable defaults apply")
    _write_state(state_path, {
        "releases": [{
            "release_id": "rel-orphan",
            "service": "orphan-svc", "environment": "prod",
            "old_version": "v1", "new_version": "v2",
            "status": "canary",
            "policy_version": 9,   # never persisted anywhere
            "steps": [{"new_version_percent": 50, "observe_minutes": 2},
                      {"new_version_percent": 100, "observe_minutes": 2}],
            "current_step_index": 0,
        }],
        "decisions": [], "history": [],
        "policies": [{  # only the default v1 is defined; v9 is dangling
            "version": 1, "name": "default", "failure_rate_threshold": 0.05,
            "min_requests": 100, "created_at": "",
            "steps": [{"new_version_percent": 100, "observe_minutes": 5}],
        }],
    })

    # Judgement must not fail: it runs under default min_requests=100 and the
    # release's own stored observe_minutes=2.
    pending = run_worker(state_path, "evaluate", {"release_id": "rel-orphan"})
    assert pending["ok"] is False and pending["code"] == "observation_pending"
    assert pending["details"]["required_minutes"] == 2
    assert pending["details"]["min_requests"] == 100
    print("RESULT: evaluate works; requires 2 minutes (stored step) and "
          ">=100 samples (stable default)")

    # Healthy window under the default threshold -> advance to step 1.
    for minute in range(2):
        run_worker(state_path, "observe", {
            "release_id": "rel-orphan", "step_index": 0,
            "minute_offset": minute, "total_requests": 60, "failed_requests": 1,
        })
    adv = run_worker(state_path, "promote", {
        "release_id": "rel-orphan", "kwargs": {"request_id": "to-step1"}})
    assert adv["to_status"] == "canary" and adv["step_index"] == 1
    assert adv["new_version_percent"] == 100
    print("RESULT: healthy window (fail_rate 1/60 < 0.05) advanced to step1")

    # Unhealthy window under the default threshold -> auto-rollback, and the
    # history snapshot records the fallback policy bound to version 9.
    for minute in range(2):
        run_worker(state_path, "observe", {
            "release_id": "rel-orphan", "step_index": 1,
            "minute_offset": minute, "total_requests": 60,
            "failed_requests": 10 if minute == 0 else 0,
        })
    rb = run_worker(state_path, "promote", {
        "release_id": "rel-orphan", "kwargs": {"request_id": "judge"}})
    assert rb["rolled_back"] is True and rb["to_status"] == "rolled_back"
    hist = run_worker(state_path, "history", {})["entries"]
    assert len(hist) == 1 and hist[0]["final_status"] == "rolled_back"
    snap = hist[0]["policy_snapshot"]
    assert snap["version"] == 9
    assert snap["failure_rate_threshold"] == 0.05
    assert snap["min_requests"] == 100
    print(f"RESULT: fail_rate 10/120 >= 0.05 -> auto-rollback; snapshot "
          f"keeps version={snap['version']} with default parameters")


def test_dangling_policy_with_no_policies_section_at_all(state_path):
    print("\nAREA 2: release bound to an undefined (non-default) policy")
    print("CASE: file has no policies section; release references v3")
    _write_state(state_path, {
        "releases": [{
            "release_id": "rel-old",
            "service": "old-svc", "environment": "prod",
            "old_version": "v1", "new_version": "v2",
            "status": "canary",
            "policy_version": 3,
            "steps": [{"new_version_percent": 100, "observe_minutes": 1}],
            "current_step_index": 0,
        }],
        "decisions": [], "history": [],
    })
    run_worker(state_path, "observe", {
        "release_id": "rel-old", "step_index": 0,
        "minute_offset": 0, "total_requests": 100, "failed_requests": 0,
    })
    done = run_worker(state_path, "promote", {
        "release_id": "rel-old", "kwargs": {"request_id": "finish"}})
    assert done["to_status"] == "completed"
    hist = run_worker(state_path, "history", {})["entries"]
    assert hist[0]["policy_snapshot"]["version"] == 3
    print("RESULT: no policies section at all -> still completes, snapshot "
          "records the originally bound version 3")


# --------------------------------------------------------------------- #
# AREA 3: repeated restarts x re-evaluation stay identical
# --------------------------------------------------------------------- #
def test_repeated_restarts_and_reevaluations_are_identical(state_path):
    print("\nAREA 3: repeated restarts x re-evaluation")
    print("CASE: same old record re-judged in 3 fresh processes -> identical")
    _write_state(state_path, {
        "releases": [{
            "release_id": "rel-legacy",
            "service": "legacy-svc", "environment": "prod",
            "old_version": "v1", "new_version": "v2",
            "status": "canary",
            # missing policy_version AND dangling would behave the same;
            # here the field is simply absent (defaults to 1, undefined here)
            "steps": [{"new_version_percent": 100, "observe_minutes": 3}],
            "current_step_index": 0,
            "observations": {"0": [
                {"minute_offset": 0, "total_requests": 40,
                 "failed_requests": 1},
            ]},
        }],
        "decisions": [], "history": [],
        # policies section deliberately omitted
    })

    snapshots = []
    for restart in range(3):
        pending = run_worker(state_path, "evaluate",
                             {"release_id": "rel-legacy"})
        assert pending["ok"] is False
        assert pending["code"] == "observation_pending"
        snapshots.append(pending["details"])
        print(f"RESTART {restart + 1}: pending details={pending['details']}")
    assert snapshots[0] == snapshots[1] == snapshots[2]
    assert snapshots[0]["required_minutes"] == 3
    assert snapshots[0]["min_requests"] == 100
    print("RESULT: threshold/minutes/sample requirements identical across "
          "3 restarts (0.05 / 3 min / 100 samples)")

    # Finish the window (healthy) in yet another fresh process, then complete.
    for minute in (1, 2):
        run_worker(state_path, "observe", {
            "release_id": "rel-legacy", "step_index": 0,
            "minute_offset": minute, "total_requests": 40,
            "failed_requests": 1,
        })
    verdicts = set()
    for restart in range(2):
        healthy = run_worker(state_path, "evaluate",
                             {"release_id": "rel-legacy"})
        verdicts.add((healthy["healthy"], healthy["reason"]))
        print(f"RESTART {restart + 1}: healthy={healthy['healthy']} "
              f"reason={healthy['reason']}")
    assert len(verdicts) == 1, "re-evaluation must be deterministic"
    done = run_worker(state_path, "promote", {
        "release_id": "rel-legacy", "kwargs": {"request_id": "finish"}})
    assert done["to_status"] == "completed"
    print("RESULT: identical healthy verdicts; release completed")


def test_finished_legacy_release_conclusion_never_rewritten(state_path):
    print("\nAREA 3: repeated restarts x re-evaluation")
    print("CASE: finished legacy release -> restarts never rewrite history")
    _write_state(state_path, {
        "releases": [{
            "release_id": "rel-done",
            "service": "done-svc", "environment": "prod",
            "old_version": "v1", "new_version": "v2",
            "status": "completed",
            "policy_version": 7,   # dangling on purpose
            "steps": [{"new_version_percent": 100, "observe_minutes": 5}],
            "current_step_index": 0,
            "finish_reason": "final observation window healthy",
        }],
        "decisions": [],
        "history": [{
            "release_id": "rel-done", "service": "done-svc",
            "environment": "prod", "old_version": "v1", "new_version": "v2",
            "final_status": "completed",
            "reason": "final observation window healthy",
            "policy_snapshot": {"version": 7, "note": "written long ago"},
            "finished_at": "2024-01-01T00:00:00+00:00",
        }],
    })
    before = run_worker(state_path, "history", {})["entries"]

    for restart in range(2):
        rel = run_worker(state_path, "get_release", {"release_id": "rel-done"})
        assert rel["status"] == "completed"
        # any new operation on the terminal release is rejected, nothing added
        again = run_worker(state_path, "promote", {
            "release_id": "rel-done", "kwargs": {"request_id": f"new{restart}"}})
        assert again["ok"] is False and again["code"] == "release_finished"
        after = run_worker(state_path, "history", {})["entries"]
        assert after == before
        print(f"RESTART {restart + 1}: still completed, promote rejected, "
              f"history byte-identical ({len(after)} entry)")
    assert len(before) == 1
    assert before[0]["policy_snapshot"] == {"version": 7,
                                            "note": "written long ago"}
    print("RESULT: conclusion + original snapshot preserved, no extra entry")
