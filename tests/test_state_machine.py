"""State machine tests: legal ramp, illegal state, step skipping, terminal ops.

Each test prints its INPUT, expected/observed TRANSITION and the engine's own
judgement logs (see conftest) so rejection reasons stay auditable.
"""

import pytest

from canary.errors import (
    IllegalStateError,
    InvalidArgumentError,
    ReleaseAlreadyExistsError,
    ReleaseFinishedError,
    ReleaseNotFoundError,
    StepSkippingError,
)
from canary.models import ReleaseStatus


def test_full_ramp_pending_to_completed(service, fill_window):
    print("\nINPUT: register svc/prod v1->v2, steps [50,100], 1-minute window")
    policy = service.create_policy("two-step", [50, 100], min_requests=10, observe_minutes=1)
    r = service.register(
        service="svc", environment="prod", old_version="v1", new_version="v2",
        policy_version=policy.version,
    )
    assert r.status == ReleaseStatus.PENDING and r.current_step_index == -1

    res = service.promote(r.release_id, request_id="enter-0")
    print(f"TRANSITION: {res.from_status.value} -> {res.to_status.value} @ {res.new_version_percent}%")
    assert res.to_status == ReleaseStatus.CANARY and res.new_version_percent == 50

    # window incomplete -> cannot continue
    from canary.errors import ObservationPendingError
    with pytest.raises(ObservationPendingError):
        service.promote(r.release_id)
    print("JUDGEMENT: window incomplete -> ObservationPendingError (no rollback)")

    fill_window(service, r.release_id, 0, minutes=1, total=100, failed=0)
    res = service.promote(r.release_id, request_id="enter-last")
    print(f"TRANSITION: {res.from_status.value} -> {res.to_status.value} @ {res.new_version_percent}%")
    assert res.to_status == ReleaseStatus.CANARY and res.new_version_percent == 100
    # the 100% step同样需要健康窗口才能完成
    with pytest.raises(ObservationPendingError):
        service.promote(r.release_id)
    print("JUDGEMENT: final-step window incomplete -> still cannot complete")
    fill_window(service, r.release_id, 1, minutes=1, total=100, failed=0)
    res = service.promote(r.release_id, request_id="complete")
    print(f"TRANSITION: {res.from_status.value} -> {res.to_status.value} (final window healthy)")
    assert res.to_status == ReleaseStatus.COMPLETED
    assert service.get_release(r.release_id).finish_reason

    history = service.history()
    assert len(history) == 1 and history[0].final_status == ReleaseStatus.COMPLETED


def test_duplicate_active_registration_rejected(service):
    print("\nINPUT: two registrations for same (svc, prod)")
    service.register(service="svc", environment="prod", old_version="v1", new_version="v2")
    with pytest.raises(ReleaseAlreadyExistsError) as ei:
        service.register(service="svc", environment="prod", old_version="v1", new_version="v3")
    print(f"REJECT: {ei.value.code} {ei.value.details}")
    assert ei.value.code == "release_already_exists"


def test_release_not_found(service):
    with pytest.raises(ReleaseNotFoundError) as ei:
        service.get_release("missing")
    assert ei.value.code == "release_not_found"


def test_invalid_registration_arguments(service):
    with pytest.raises(InvalidArgumentError):
        service.register(service="", environment="prod", old_version="v1", new_version="v2")
    with pytest.raises(InvalidArgumentError):
        service.register(service="s", environment="prod", old_version="v1", new_version="v1")
    print("\nINPUT: empty service / identical versions -> invalid_argument rejected")


def test_step_skipping_is_rejected(service):
    policy = service.create_policy("four", [10, 30, 50, 100])
    r = service.register(
        service="s", environment="dev", old_version="v1", new_version="v2",
        policy_version=policy.version,
    )
    print("\nINPUT: promote with step_index=2 while next is 0")
    service.promote(r.release_id, request_id="p0")
    with pytest.raises(StepSkippingError) as ei:
        service.promote(r.release_id, step_index=2)
    print(f"REJECT: {ei.value.code} {ei.value.details}")
    assert ei.value.details["expected_next_step"] == 1
    # re-specifying the current step as target is also skipping (must be next)
    with pytest.raises(StepSkippingError):
        service.promote(r.release_id, step_index=0)
    print("REJECT: re-promoting current step -> step_skipping")


def test_operations_on_terminal_release_rejected_with_distinct_code(service, fill_window):
    policy = service.create_policy("fast", [100], min_requests=1, observe_minutes=1)
    r = service.register(
        service="s", environment="qa", old_version="v1", new_version="v2",
        policy_version=policy.version,
    )
    service.promote(r.release_id, request_id="enter")
    fill_window(service, r.release_id, 0, minutes=1, total=10, failed=0)
    done = service.promote(r.release_id, request_id="finish")
    assert done.to_status == ReleaseStatus.COMPLETED
    print("\nINPUT: promote & rollback on COMPLETED release")
    with pytest.raises(ReleaseFinishedError) as ei:
        service.promote(r.release_id, request_id="after-1")
    print(f"REJECT promote: {ei.value.code}")
    with pytest.raises(ReleaseFinishedError) as ei:
        service.rollback(r.release_id, request_id="after-2")
    print(f"REJECT rollback: {ei.value.code}")


def test_observation_for_future_step_or_terminal_rejected(service):
    policy = service.create_policy("p", [10, 100])
    r = service.register(
        service="s", environment="qa", old_version="v1", new_version="v2",
        policy_version=policy.version,
    )
    from canary.models import ObservationPoint
    with pytest.raises(IllegalStateError) as ei:
        service.observe(
            r.release_id, 0, ObservationPoint(0, 100, 1)
        )
    print(f"\nREJECT observe before entering step: {ei.value.code}")
    service.promote(r.release_id)
    service.rollback(r.release_id, reason="manual")
    with pytest.raises(ReleaseFinishedError):
        service.observe(r.release_id, 0, ObservationPoint(0, 100, 1))
    print("REJECT observe after rollback: release_finished")
