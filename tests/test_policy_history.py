"""Policy versioning & historical reproduction tests."""

import copy

import pytest

from canary.errors import InvalidPolicyError, PolicyNotFoundError
from canary.models import ObservationPoint, ReleaseStatus
from canary.policy import (
    DEFAULT_FAILURE_RATE_THRESHOLD,
    DEFAULT_MIN_REQUESTS,
    DEFAULT_OBSERVE_MINUTES,
    PromotionPolicy,
)


def test_policy_versions_are_immutable_and_numbered(service):
    p1 = service.create_policy("a", [10, 100], failure_rate_threshold=0.1)
    p2 = service.create_policy("b", [20, 80, 100], failure_rate_threshold=0.02)
    # service seeds a default v1, so new policies start at v2
    assert p1.version == 2 and p2.version == 3
    with pytest.raises(Exception):
        p1.name = "mutated"  # frozen dataclass
    print(f"\nPOLICY: v1 threshold={p1.failure_rate_threshold}, v2 threshold={p2.failure_rate_threshold}")
    with pytest.raises(PolicyNotFoundError):
        service.policies.get(99)


def test_invalid_policy_shapes_rejected(service):
    for bad in ([], [0, 100], [50, 40, 100], [10, 99], [101]):
        with pytest.raises(InvalidPolicyError):
            service.create_policy("bad", bad)
        print(f"REJECT invalid steps {bad} -> invalid_policy")
    with pytest.raises(InvalidPolicyError):
        service.create_policy("bad", [100], failure_rate_threshold=1.5)


def test_legacy_records_missing_fields_use_stable_defaults():
    # very old record: bare-int steps, no threshold/min_requests/name
    old = PromotionPolicy.from_dict({"version": 1, "steps": [10, 100]})
    assert old.failure_rate_threshold == DEFAULT_FAILURE_RATE_THRESHOLD == 0.05
    assert old.min_requests == DEFAULT_MIN_REQUESTS == 100
    assert old.name == "default"
    assert [s.observe_minutes for s in old.steps] == [
        DEFAULT_OBSERVE_MINUTES,
        DEFAULT_OBSERVE_MINUTES,
    ]
    print("\nLEGACY: missing fields -> stable defaults, not current values")
    # even older: no steps at all
    oldest = PromotionPolicy.from_dict({"version": 0})
    assert len(oldest.steps) == 4 and oldest.steps[-1].new_version_percent == 100
    print(f"LEGACY: no steps -> default ramp {[s.new_version_percent for s in oldest.steps]}")


def test_same_input_same_policy_is_deterministic():
    raw = {
        "version": 7,
        "name": "x",
        "steps": [
            {"new_version_percent": 10, "observe_minutes": 3},
            {"new_version_percent": 100, "observe_minutes": 7},
        ],
        "failure_rate_threshold": 0.125,
        "min_requests": 250,
        "created_at": "t",
    }
    a = PromotionPolicy.from_dict(copy.deepcopy(raw))
    b = PromotionPolicy.from_dict(copy.deepcopy(raw))
    assert a == b and a.to_dict() == b.to_dict()
    print("\nDETERMINISM: identical stored input -> identical policy/conclusion input")


def _finish_under_policy(service, *, policy, failed_per_minute, request_prefix):
    r = service.register(
        service=f"svc-{request_prefix}", environment="prod",
        old_version="v1", new_version="v2", policy_version=policy.version,
    )
    service.promote(r.release_id, request_id=f"{request_prefix}-enter")
    for idx, step in enumerate(policy.steps):
        for m in range(step.observe_minutes):
            service.observe(
                r.release_id, idx,
                ObservationPoint(m, 1000, failed_per_minute),
            )
        result = service.promote(r.release_id, request_id=f"{request_prefix}-{idx}")
        if getattr(result, "rolled_back", False):
            break
    return r, result


def test_policy_change_does_not_rewrite_finished_history(service):
    # v1: lenient threshold 0.20 -> a release seeing 10% failure COMPLETES.
    v1 = service.create_policy("lenient", [50, 100], failure_rate_threshold=0.20,
                               min_requests=10, observe_minutes=1)
    r1, done1 = _finish_under_policy(
        service, policy=v1, failed_per_minute=100, request_prefix="a"  # 10% < 20%
    )
    assert done1.to_status == ReleaseStatus.COMPLETED
    snapshot_v1 = service.history()[-1].policy_snapshot
    print(f"\nHISTORY under v1: {done1.to_status.value}, snapshot threshold=0.20")

    # v2: tighten threshold to 0.05 and change steps/duration.
    v2 = service.create_policy("strict", [10, 30, 100], failure_rate_threshold=0.05,
                               min_requests=500, observe_minutes=3)
    # same 10% failure under v2 must auto-rollback
    r2, done2 = _finish_under_policy(
        service, policy=v2, failed_per_minute=100, request_prefix="b"  # 10% >= 5%
    )
    assert done2.rolled_back and done2.to_status == ReleaseStatus.ROLLED_BACK
    print(f"NEW release under v2: {done2.to_status.value} ({done2.reason[:50]}...)")

    history = service.history()
    by_id = {h.release_id: h for h in history}
    # the finished v1 release keeps its COMPLETED conclusion and v1 snapshot
    assert by_id[r1.release_id].final_status == ReleaseStatus.COMPLETED
    assert by_id[r1.release_id].policy_snapshot == snapshot_v1
    assert by_id[r1.release_id].policy_snapshot["failure_rate_threshold"] == 0.20
    assert by_id[r2.release_id].final_status == ReleaseStatus.ROLLED_BACK
    assert by_id[r2.release_id].policy_snapshot["failure_rate_threshold"] == 0.05
    print("IMMUTABILITY: old conclusion stays COMPLETED despite newer strict policy")

    # deterministic replay: reconstructing the v1 snapshot yields the same data
    replay = PromotionPolicy.from_dict(snapshot_v1)
    assert replay.to_dict() == snapshot_v1
    assert replay.failure_rate_threshold == 0.20
    print("REPLAY: policy snapshot rebuilt from history byte-for-byte equivalent")


def test_policy_change_keeps_conclusions_consistent_across_reruns(service):
    v1 = service.create_policy("p", [100], failure_rate_threshold=0.3,
                               min_requests=1, observe_minutes=1)
    results = set()
    for i in range(3):
        r, done = _finish_under_policy(
            service, policy=v1, failed_per_minute=20, request_prefix=f"k{i}"
        )  # 20% < 30% -> healthy every time
        results.add((done.to_status, done.rolled_back))
        assert done.to_status == ReleaseStatus.COMPLETED
    assert results == {(ReleaseStatus.COMPLETED, False)}
    print("\nREPEATABILITY: same inputs+policy over 3 releases -> same conclusions")
