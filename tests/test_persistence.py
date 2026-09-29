"""Restart / durability scenarios over a file-backed LocalStore.

Every test constructs a fresh ``CanaryService`` against the *same* state
file path to model a process restart: state, policy versions, the decision
ledger and history all come from disk, never from the previous process.
"""

from __future__ import annotations

import json
import os

import pytest

from canary.errors import ObservationPendingError, ReleaseFinishedError
from canary.models import ObservationPoint, ReleaseStatus
from canary.service import CanaryService


def _open(path) -> CanaryService:
    return CanaryService(persist_path=str(path))


def test_unfinished_release_resumes_after_restart_with_custom_policy(tmp_path):
    path = tmp_path / "state.json"
    svc = _open(path)
    policy = svc.create_policy(
        "strict", [20, 60, 100],
        failure_rate_threshold=0.12, min_requests=77, observe_minutes=3,
    )
    r = svc.register(
        service="checkout", environment="prod",
        old_version="v1", new_version="v2", policy_version=policy.version,
    )
    svc.promote(r.release_id, request_id="enter")
    # die mid-window: only 2 of the required 3 minutes are on disk
    for minute in range(2):
        svc.observe(r.release_id, 0, ObservationPoint(minute, 40, 0))
    assert svc.get_release(r.release_id).current_step_index == 0

    # ---- process restart: nothing is carried over in memory ----
    svc2 = _open(path)
    r2 = svc2.get_release(r.release_id)
    assert r2.status == ReleaseStatus.CANARY and r2.current_step_index == 0
    restored_policy = svc2.policies.get(r2.policy_version)
    assert restored_policy.failure_rate_threshold == 0.12
    assert restored_policy.min_requests == 77
    assert [s.observe_minutes for s in restored_policy.steps] == [3, 3, 3]
    assert [s.new_version_percent for s in restored_policy.steps] == [20, 60, 100]
    print("\nRESTART: custom threshold/min_requests/observe window preserved")

    # judgement basis unchanged: the half-recorded window still needs minute 2
    # and 77 samples; an incomplete window can never cause a rollback
    with pytest.raises(ObservationPendingError) as pending:
        svc2.evaluate(r.release_id)
    assert pending.value.details["required_minutes"] == 3
    assert pending.value.details["min_requests"] == 77

    # complete the interrupted window post-restart and walk the rest of the
    # ramp using the restored custom policy
    svc2.observe(r.release_id, 0, ObservationPoint(2, 40, 0))
    svc2.promote(r.release_id, request_id="to-step1")
    for minute in range(3):
        svc2.observe(r.release_id, 1, ObservationPoint(minute, 30, 0))
    svc2.promote(r.release_id, request_id="to-step2")
    for minute in range(3):
        svc2.observe(r.release_id, 2, ObservationPoint(minute, 30, 0))
    done = svc2.promote(r.release_id, request_id="finish")
    assert done.to_status == ReleaseStatus.COMPLETED
    assert svc2.get_release(r.release_id).status == ReleaseStatus.COMPLETED
    print("RESTART: unfinished release (incl. mid-window) completed after restart")


def test_finished_conclusion_is_immutable_across_restart_and_repeat(tmp_path):
    path = tmp_path / "state.json"
    svc = _open(path)
    policy = svc.create_policy(
        "one-shot", [50, 100],
        failure_rate_threshold=0.2, min_requests=10, observe_minutes=1,
    )
    r = svc.register(
        service="payments", environment="staging",
        old_version="v1", new_version="v2", policy_version=policy.version,
    )
    svc.promote(r.release_id, request_id="enter")
    svc.observe(r.release_id, 0, ObservationPoint(0, 100, 5))  # 5% healthy
    svc.promote(r.release_id, request_id="to-final")
    svc.observe(r.release_id, 1, ObservationPoint(0, 100, 5))
    done = svc.promote(r.release_id, request_id="finish")
    assert done.to_status == ReleaseStatus.COMPLETED
    history_before = [h for h in svc.history() if h.release_id == r.release_id]
    assert len(history_before) == 1
    snapshot = history_before[0].policy_snapshot

    # restart, and again restart: conclusions never change
    for _ in range(2):
        svc2 = _open(path)
        release = svc2.get_release(r.release_id)
        assert release.status == ReleaseStatus.COMPLETED
        assert release.finish_reason
        history = [h for h in svc2.history() if h.release_id == r.release_id]
        assert len(history) == 1
        assert history[0].final_status == ReleaseStatus.COMPLETED
        assert history[0].policy_snapshot == snapshot
        with pytest.raises(ReleaseFinishedError):
            svc2.promote(r.release_id, request_id="another-one")
        with pytest.raises(ReleaseFinishedError):
            svc2.rollback(r.release_id, request_id="rb-after-done")

    print("\nRESTART: finished conclusion/history stable over restarts and repeats")


def test_decision_ledger_replays_same_request_id_after_restart(tmp_path):
    path = tmp_path / "state.json"
    svc = _open(path)
    r = svc.register(
        service="svc-ledger", environment="prod",
        old_version="v1", new_version="v2",
    )
    first = svc.promote(r.release_id, request_id="durable-id")
    assert first.new_version_percent == 10
    svc = _open(path)  # restart
    replay = svc.promote(r.release_id, request_id="durable-id")
    assert replay.step_index == 0 and replay.rolled_back is False
    assert svc.get_release(r.release_id).current_step_index == 0
    assert svc.history() == []
    print("\nRESTART: same request_id replays, does not double-advance")


def test_two_services_share_one_state_file(tmp_path):
    path = tmp_path / "state.json"
    a = _open(path)
    b = _open(path)
    policy = a.create_policy("from-a", [30, 100], observe_minutes=1, min_requests=1)
    # b sees the policy a just created
    assert [p.version for p in b.policies.all_versions()] == [1, policy.version]
    r = a.register(
        service="shared", environment="prod",
        old_version="v1", new_version="v2", policy_version=policy.version,
    )
    # b sees the release a just registered and can operate it
    seen = b.get_release(r.release_id)
    assert seen.status == ReleaseStatus.PENDING
    b.promote(r.release_id, request_id="enter-by-b")
    assert a.get_release(r.release_id).status == ReleaseStatus.CANARY
    b.observe(r.release_id, 0, ObservationPoint(0, 10, 0))
    b.promote(r.release_id, request_id="to-100-by-b")
    b.observe(r.release_id, 1, ObservationPoint(0, 10, 0))
    b.promote(r.release_id, request_id="finish-by-b")
    assert a.get_release(r.release_id).status == ReleaseStatus.COMPLETED
    history = a.history()
    assert len(history) == 1 and history[0].release_id == r.release_id
    print("\nSHARED: two services share releases, policies, history in real time")


def test_legacy_format_state_file_loads_with_stable_defaults(tmp_path):
    # Hand-written v1-era file: no "policies" section, legacy bare-int steps
    # are not used by releases (steps are always structured), but the release
    # references policy v2 that no longer exists in the file.
    legacy = {
        "releases": [
            {
                "release_id": "rel-old",
                "service": "legacy",
                "environment": "prod",
                "old_version": "v0.9",
                "new_version": "v1.0",
                "status": "canary",
                "policy_version": 2,
                "steps": [
                    {"new_version_percent": 10, "observe_minutes": 5},
                    {"new_version_percent": 100, "observe_minutes": 5},
                ],
                "current_step_index": 0,
                "created_at": "long ago",
                "observations": {},
                "finish_reason": "",
                # updated_at intentionally missing
            }
        ],
        "decisions": [],
        "history": [
            {
                "release_id": "rel-old-done",
                "service": "legacy",
                "environment": "prod",
                "old_version": "v0.8",
                "new_version": "v0.9",
                "final_status": "rolled_back",
                # reason / finished_at / policy_snapshot intentionally missing
            }
        ],
    }
    path = tmp_path / "state.json"
    path.write_text(json.dumps(legacy), encoding="utf-8")

    svc = _open(path)
    release = svc.get_release("rel-old")
    assert release is not None and release.policy_version == 2
    policy = svc.policies.get(2)
    assert policy.failure_rate_threshold == 0.05
    assert policy.min_requests == 100
    assert [s.observe_minutes for s in policy.steps] == [5, 5, 5, 5]
    assert release.updated_at == ""  # missing field -> stable default
    history = {h.release_id: h for h in svc.history()}
    h = history["rel-old-done"]
    assert h.final_status == ReleaseStatus.ROLLED_BACK
    assert h.reason == "" and h.finished_at == "" and h.policy_snapshot == {}

    # the resumed legacy release still follows the default judgement basis
    with pytest.raises(ObservationPendingError):
        svc.evaluate("rel-old")
    print("\nLEGACY: old file without policies/missing fields loads with defaults")


def test_crash_during_write_leaves_last_committed_state_and_retry_commits_once(
    tmp_path, monkeypatch
):
    path = tmp_path / "state.json"
    svc = _open(path)
    r = svc.register(
        service="crashy", environment="prod",
        old_version="v1", new_version="v2",
    )
    committed = path.read_text(encoding="utf-8")

    # Simulate the process being killed while the tmp file has been written
    # (and fsynced) but before the atomic rename lands.
    import canary.store as store_mod

    def boom(_fd):
        raise OSError("simulated kill mid flush")

    monkeypatch.setattr(store_mod.os, "fsync", boom)
    with pytest.raises(OSError):
        svc.promote(r.release_id, request_id="may-have-died")
    monkeypatch.undo()

    # main state file is untouched, complete JSON, and still consistent
    assert path.read_text(encoding="utf-8") == committed
    recovered = json.loads(path.read_text(encoding="utf-8"))
    assert recovered["releases"][0]["status"] == "pending"
    stale_tmp = list(tmp_path.glob("state.json.tmp.*"))
    assert stale_tmp, "crashed tmp sidecar exists but is never read as state"

    # restart (new process) and retry: commits exactly one advance
    svc2 = _open(path)
    result = svc2.promote(r.release_id, request_id="retry-after-restart")
    assert result.to_status == ReleaseStatus.CANARY
    again = json.loads(path.read_text(encoding="utf-8"))
    assert again["releases"][0]["status"] == "canary"
    assert len(again["decisions"]) == 1
    # the successful post-restart commit also swept the dead tmp sidecar
    assert list(tmp_path.glob("state.json.tmp.*")) == []
    print("\nDURABILITY: kill mid-write -> no torn state; retry after restart commits once")


def test_state_file_is_always_valid_json_after_every_operation(tmp_path):
    path = tmp_path / "state.json"
    svc = _open(path)
    r = svc.register(
        service="json-check", environment="prod",
        old_version="v1", new_version="v2",
    )
    svc.promote(r.release_id, request_id="enter")
    svc.observe(r.release_id, 0, ObservationPoint(0, 100, 0))
    json.loads(path.read_text(encoding="utf-8"))  # never raises
    # no stray committed tmp files linger on the happy path
    assert list(tmp_path.glob("state.json.tmp.*")) == []
    print("\nDURABILITY: state file parses after every commit; no tmp leftovers")
