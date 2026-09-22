"""Observation window & threshold boundary tests."""

import pytest

from canary.errors import ObservationPendingError
from canary.models import ObservationPoint, ReleaseStatus


def _start(service, **policy_kw):
    params = {"min_requests": 100, "observe_minutes": 2}
    params.update(policy_kw)
    policy = service.create_policy("p", [50, 100], **params)
    r = service.register(
        service="s", environment="prod", old_version="v1", new_version="v2",
        policy_version=policy.version,
    )
    service.promote(r.release_id)
    return r


def test_empty_and_partial_window_never_judges(service):
    r = _start(service)
    print("\nINPUT: promote with zero observations -> must not rollback")
    with pytest.raises(ObservationPendingError) as ei:
        service.promote(r.release_id)
    assert ei.value.details["window_full"] is False
    print(f"PENDING details={ei.value.details}")

    service.observe(r.release_id, 0, ObservationPoint(0, 1000, 999))
    print("INPUT: 1/2 minutes even at 99.9% failure -> still no rollback")
    with pytest.raises(ObservationPendingError):
        service.promote(r.release_id)
    assert service.get_release(r.release_id).status == ReleaseStatus.CANARY
    print("STATUS: still canary (window incomplete protects from false rollback)")


def test_insufficient_samples_pending_even_window_full(service):
    r = _start(service)
    print("\nINPUT: full 2-minute window but 99/100 total requests")
    service.observe(r.release_id, 0, ObservationPoint(0, 50, 0))
    service.observe(r.release_id, 0, ObservationPoint(1, 49, 49))
    with pytest.raises(ObservationPendingError) as ei:
        service.promote(r.release_id)
    assert ei.value.details["enough_samples"] is False
    print(f"PENDING details={ei.value.details}")
    # a 100% fail rate under too few samples still cannot trigger rollback
    assert service.get_release(r.release_id).status == ReleaseStatus.CANARY


def test_threshold_boundary_equals_means_unhealthy(service):
    # threshold defaults to 0.05; boundary fail_rate == threshold must rollback
    r = _start(service, failure_rate_threshold=0.05)
    print("\nINPUT: 2 minutes x (100 total, 5 failed) => fail_rate=0.05 == threshold")
    service.observe(r.release_id, 0, ObservationPoint(0, 100, 5))
    service.observe(r.release_id, 0, ObservationPoint(1, 100, 5))
    result = service.promote(r.release_id, request_id="boundary")
    assert result.rolled_back is True
    assert result.to_status == ReleaseStatus.ROLLED_BACK
    assert "fail_rate=0.050000 threshold=0.05" in result.reason
    print(f"AUTO ROLLBACK reason={result.reason}")


def test_just_below_threshold_is_healthy(service):
    r = _start(service, failure_rate_threshold=0.05)
    print("\nINPUT: 200 total, 9 failed => fail_rate=0.045 < 0.05")
    service.observe(r.release_id, 0, ObservationPoint(0, 100, 4))
    service.observe(r.release_id, 0, ObservationPoint(1, 100, 5))
    result = service.promote(r.release_id, request_id="healthy")
    assert result.rolled_back is False and result.to_status == ReleaseStatus.CANARY
    assert result.new_version_percent == 100
    print(f"ADVANCED -> {result.to_status.value} @ {result.new_version_percent}%")


def test_evaluate_deterministic_and_explicit(service):
    r = _start(service, failure_rate_threshold=0.1)
    service.observe(r.release_id, 0, ObservationPoint(0, 100, 50))
    service.observe(r.release_id, 0, ObservationPoint(1, 100, 50))
    h1, why1 = service.evaluate(r.release_id)
    h2, why2 = service.evaluate(r.release_id)
    print(f"\nJUDGEMENT healthy={h1} reason={why1}")
    assert (h1, why1) == (h2, why2) and h1 is False
    print("determinism: repeated evaluate => identical conclusion")


def test_min_requests_zero_uses_window_only(service):
    r = _start(service, min_requests=0)
    print("\nINPUT: min_requests=0; full window with tiny traffic 1 req/fail 1")
    service.observe(r.release_id, 0, ObservationPoint(0, 1, 1))
    service.observe(r.release_id, 0, ObservationPoint(1, 0, 0))
    result = service.promote(r.release_id, request_id="zero-min")
    # fail_rate = 1.0 >= 0.05 -> rollback
    assert result.rolled_back is True
    print(f"AUTO ROLLBACK at fail_rate=1.0: {result.reason}")
