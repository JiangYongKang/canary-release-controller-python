"""Cross-process concurrency tests.

These spawn real separate OS processes (the CLI over one shared state file)
so the flock + durable-decision-ledger guarantees are exercised exactly as
they are between a resident service and a background/CLI script.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run_cli(state, *args, timeout=30):
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, "-m", "canary.cli.main", "--state", str(state), *args]
    return subprocess.run(
        cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=timeout
    )


def _state_view(state, release_id):
    data = json.loads(state.read_text(encoding="utf-8"))
    release = next(r for r in data["releases"] if r["release_id"] == release_id)
    history = [h for h in data["history"] if h["release_id"] == release_id]
    decisions = [d for d in data["decisions"] if d["release_id"] == release_id]
    return release, history, decisions


def _spawn(*a, **kw):
    """Start one CLI process without waiting."""
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    state = a[0]
    args = a[1]
    cmd = [sys.executable, "-m", "canary.cli.main", "--state", str(state), *args]
    return subprocess.Popen(
        cmd, cwd=REPO_ROOT, env=env, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )


@pytest.fixture
def ready_release(tmp_path):
    """Register a release on a 1-minute, low-sample policy and enter step 0.

    Returns ``(state_path, release_id)``; each later action runs in its own
    freshly spawned process.
    """
    state = tmp_path / "multi.json"
    pol = _run_cli(state, "policy-create", "--name", "fast", "--steps", "10,50,100",
                   "--threshold", "0.05", "--min-requests", "10", "--observe-minutes", "1")
    assert pol.returncode == 0, pol.stderr
    version = json.loads(pol.stdout)["version"]
    reg = _run_cli(state, "register", "--service", "multi", "--environment", "prod",
                   "--old", "v1", "--new", "v2", "--policy-version", str(version))
    assert reg.returncode == 0, reg.stderr
    rid = json.loads(reg.stdout)["release_id"]
    entered = _run_cli(state, "promote", rid, "--request-id", "enter")
    assert entered.returncode == 0, entered.stderr
    # fill the step-0 window so the release is exactly one promote from step 1
    obs = _run_cli(state, "observe", rid, "--step", "0", "--minute", "0",
                   "--total", "100", "--failed", "0")
    assert obs.returncode == 0, obs.stderr
    return state, rid


def test_concurrent_promote_same_request_id_across_processes(ready_release):
    state, rid = ready_release
    procs = [_spawn(state, ("promote", rid, "--request-id", "shared")) for _ in range(8)]
    exits, stdout, errs = [], [], []
    for p in procs:
        out, err = p.communicate(timeout=30)
        exits.append(p.returncode)
        if out.strip():
            stdout.append(json.loads(out))
        if p.returncode != 0:
            errs.append(err)
    assert exits == [0] * 8, errs
    steps = sorted(d["step_index"] for d in stdout)
    rolled_back = [d for d in stdout if d["rolled_back"]]
    assert not rolled_back
    # the winner advances 0 -> 1; losers replay the *same stored conclusion*,
    # so every process reports step_index 1 while the ramp moves exactly once
    assert steps == [1] * 8, steps
    release, history, decisions = _state_view(state, rid)
    assert release["status"] == "canary" and release["current_step_index"] == 1
    assert history == []
    shared = [d for d in decisions if d["request_id"] == "shared"]
    assert len(shared) == 1
    print("\nMULTI: 8 processes same request_id -> one advance, one ledger row, no history")


def test_concurrent_promote_distinct_ids_one_winner_across_processes(ready_release):
    state, rid = ready_release
    procs = [_spawn(state, ("promote", rid, "--request-id", f"p{i}")) for i in range(8)]
    winners, pending = [], []
    for p in procs:
        out, _ = p.communicate(timeout=30)
        body = json.loads(out)
        if p.returncode == 0:
            winners.append(body["step_index"])
        else:
            pending.append(body["error"]["code"])
    assert winners == [1], winners
    assert set(pending) == {"observation_pending"}, pending
    release, history, decisions = _state_view(state, rid)
    assert release["current_step_index"] == 1
    assert history == []  # mid-ramp, no conclusion
    print("\nMULTI: 8 processes distinct ids -> exactly one advance, rest observation_pending")


def test_concurrent_promote_and_rollback_unhealthy_across_processes(ready_release):
    state, rid = ready_release
    # replace the healthy step-0 window with an unhealthy one (10% >= 5%):
    # a fresh state file reuses the same release but we rebuild it unhealthy.
    data = json.loads(state.read_text(encoding="utf-8"))
    release = next(r for r in data["releases"] if r["release_id"] == rid)
    release["observations"]["0"][0]["failed_requests"] = 10  # 10 of 100
    state.write_text(json.dumps(data), encoding="utf-8")

    workers = [("promote", f"p{i}") for i in range(6)]
    workers += [("rollback", f"r{i}") for i in range(6)]
    procs = [
        _spawn(state, (kind, rid, "--request-id", req)) for kind, req in workers
    ]
    conclusions, rejects = [], []
    for p in procs:
        out, _ = p.communicate(timeout=30)
        body = json.loads(out)
        if p.returncode == 0:
            conclusions.append(body)
        else:
            rejects.append(body["error"]["code"])
    # everyone that commits sees rolled_back; losers see release_finished
    assert all(d["to_status"] == "rolled_back" for d in conclusions), conclusions
    assert set(rejects) <= {"release_finished"}, rejects
    assert len(conclusions) >= 1
    release, history, decisions = _state_view(state, rid)
    assert release["status"] == "rolled_back"
    assert release["current_step_index"] == 0  # percent never crept upwards
    assert len(history) == 1, history  # rollback recorded exactly once
    print(f"\nMULTI: 6 promote + 6 rollback on unhealthy -> one rolled_back history,")
    print(f"       {len(conclusions)} same conclusions, {len(rejects)} release_finished")


def test_concurrent_rollback_only_happens_once_across_processes(ready_release):
    state, rid = ready_release
    procs = [
        _spawn(state, ("rollback", rid, "--reason", "manual",
                       "--request-id", f"rb{i}"))
        for i in range(8)
    ]
    rolled_back, finished = 0, 0
    for p in procs:
        out, _ = p.communicate(timeout=30)
        body = json.loads(out)
        if p.returncode == 0:
            assert body["to_status"] == "rolled_back"
            rolled_back += 1
        else:
            assert body["error"]["code"] == "release_finished"
            finished += 1
    assert rolled_back == 1 and finished == 7
    release, history, decisions = _state_view(state, rid)
    assert release["status"] == "rolled_back"
    assert len(history) == 1
    print("\nMULTI: 8 concurrent rollbacks -> exactly one rollback + one history entry")


def test_restart_retry_same_request_commits_one_conclusion(tmp_path):
    state = tmp_path / "restart.json"
    def run(*args):
        return _run_cli(state, *args)

    run("policy-create", "--name", "fast", "--steps", "50,100",
        "--min-requests", "1", "--observe-minutes", "1")
    rid = json.loads(run("register", "--service", "r", "--environment", "prod",
                         "--old", "v1", "--new", "v2", "--policy-version", "2").stdout)["release_id"]
    run("promote", rid, "--request-id", "enter")
    run("observe", rid, "--step", "0", "--minute", "0", "--total", "10", "--failed", "0")

    # first attempt finishes the release in one process; a *new* process retries
    # the same request id after "restart"
    first = run("promote", rid, "--request-id", "to-final")
    assert first.returncode == 0, first.stdout
    retried = run("promote", rid, "--request-id", "to-final")
    assert retried.returncode == 0
    assert json.loads(retried.stdout)["step_index"] == 1
    data = json.loads(state.read_text(encoding="utf-8"))
    rel = next(r for r in data["releases"] if r["release_id"] == rid)
    assert rel["status"] == "canary" and rel["current_step_index"] == 1
    rows = [d for d in data["decisions"]
            if d["release_id"] == rid and d["request_id"] == "to-final"]
    assert len(rows) == 1  # no duplicated ledger row from the post-restart retry
    assert len(data["history"]) == 0  # still mid-ramp, no premature conclusion

    # finish for real; a later restart never mutates the concluded history
    run("observe", rid, "--step", "1", "--minute", "0", "--total", "10", "--failed", "0")
    done = run("promote", rid, "--request-id", "finish")
    assert json.loads(done.stdout)["to_status"] == "completed"
    again = _run_cli(state, "get", rid)
    assert json.loads(again.stdout)["status"] == "completed"
    data = json.loads(state.read_text(encoding="utf-8"))
    finals = [h for h in data["history"] if h["release_id"] == rid]
    assert len(finals) == 1 and finals[0]["final_status"] == "completed"
    print("\nMULTI: restart + retry same request -> single ledger row, single conclusion")


def test_policy_versions_created_in_one_process_are_visible_in_others(tmp_path):
    state = tmp_path / "policies.json"
    created_proc = _run_cli(
        state, "policy-create", "--name", "from-script", "--steps", "25,75,100",
        "--threshold", "0.08", "--min-requests", "33", "--observe-minutes", "2",
    )
    assert created_proc.returncode == 0, created_proc.stderr
    version = json.loads(created_proc.stdout)["version"]
    # a brand-new process lists and immediately registers under that version
    listed_proc = _run_cli(state, "policy-list")
    assert listed_proc.returncode == 0
    versions = [p["version"] for p in json.loads(listed_proc.stdout)["policies"]]
    assert 1 in versions and version in versions
    reg_proc = _run_cli(
        state, "register", "--service", "pv", "--environment", "prod",
        "--old", "v1", "--new", "v2", "--policy-version", str(version),
    )
    assert reg_proc.returncode == 0, reg_proc.stderr
    rid = json.loads(reg_proc.stdout)["release_id"]
    # and a third process keeps using the same durable custom policy
    entered_proc = _run_cli(state, "promote", rid, "--request-id", "enter")
    assert entered_proc.returncode == 0
    assert json.loads(entered_proc.stdout)["new_version_percent"] == 25
    data = json.loads(state.read_text(encoding="utf-8"))
    assert data["releases"][0]["policy_version"] == version
    print("\nMULTI: policy created in one process is listable/usable in others, persisted")
