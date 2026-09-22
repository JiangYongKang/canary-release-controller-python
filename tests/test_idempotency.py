"""Concurrency & idempotency tests.

Uses real threads driven by a barrier so all workers start in the same
instant; the engine's single store-lock transaction plus the decision ledger
must guarantee: no double rollback, no missed rollback, no torn conclusions.
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest

from canary.errors import (
    IllegalStateError,
    ReleaseFinishedError,
    StepSkippingError,
)
from canary.models import ObservationPoint, ReleaseStatus


def _ready_release(service, *, minutes=2, total=100, failed=0, threshold=0.05):
    policy = service.create_policy(
        "p", [10, 50, 100],
        failure_rate_threshold=threshold, min_requests=100, observe_minutes=minutes,
    )
    r = service.register(
        service=f"s-{datetime.now().timestamp()}",
        environment="prod", old_version="v1", new_version="v2",
        policy_version=policy.version,
    )
    service.promote(r.release_id)
    for m in range(minutes):
        service.observe(r.release_id, 0, ObservationPoint(m, total, failed))
    return r


def test_concurrent_promote_same_request_id_single_advance(service):
    r = _ready_release(service, failed=0)
    print(f"\nINPUT: 20 concurrent promote(request_id='same') on {r.release_id}")
    barrier = threading.Barrier(20)

    def call():
        barrier.wait()
        return service.promote(r.release_id, request_id="same")

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(lambda _: call(), range(20)))
    statuses = {(x.to_status, x.step_index, x.rolled_back) for x in results}
    assert statuses == {(ReleaseStatus.CANARY, 1, False)}, statuses
    release = service.get_release(r.release_id)
    assert release.status == ReleaseStatus.CANARY and release.current_step_index == 1
    assert service.history() == []  # not finished -> no conclusion written
    print("TRANSITION: exactly one advance to step 1; 19 idempotent replays; no history")


def test_concurrent_promote_distinct_ids_midramp(service):
    r = _ready_release(service, failed=0)
    print(f"\nINPUT: 16 concurrent promote with *distinct* request_ids (mid-ramp)")
    barrier = threading.Barrier(16)
    outcomes = []
    lock = threading.Lock()

    def call(i):
        barrier.wait()
        try:
            res = service.promote(r.release_id, request_id=f"r{i}")
            with lock:
                outcomes.append(("ok", res.to_status.value, res.step_index))
        except Exception as e:
            with lock:
                outcomes.append(("reject", type(e).__name__, getattr(e, "code", "")))

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(call, range(16)))
    wins = [o for o in outcomes if o[0] == "ok"]
    rejects = [o for o in outcomes if o[0] == "reject"]
    # exactly one call advances; the rest find the new window not yet full
    assert len(wins) == 1, wins
    assert wins[0][1] == ReleaseStatus.CANARY.value and wins[0][2] == 1
    assert all(o[1] == "ObservationPendingError" and o[2] == "observation_pending"
               for o in rejects), rejects
    assert service.get_release(r.release_id).current_step_index == 1
    print(f"OUTCOMES: 1 winner + {len(rejects)} observation_pending; state not torn")


def test_concurrent_final_promote_distinct_ids_one_completion(service):
    # Drive the release to the final 100% step with a healthy window, so the
    # release is exactly one promote away from COMPLETED.
    r = _ready_release(service, failed=0)
    service.promote(r.release_id, request_id="to-1")
    for m in range(2):
        service.observe(r.release_id, 1, ObservationPoint(m, 100, 0))
    service.promote(r.release_id, request_id="to-2")
    for m in range(2):
        service.observe(r.release_id, 2, ObservationPoint(m, 100, 0))
    print(f"\nINPUT: 16 concurrent *finishing* promotes, distinct request_ids")
    barrier = threading.Barrier(16)
    outcomes = []
    lock = threading.Lock()

    def call(i):
        barrier.wait()
        try:
            res = service.promote(r.release_id, request_id=f"f{i}")
            with lock:
                outcomes.append(("ok", res.to_status.value))
        except Exception as e:
            with lock:
                outcomes.append(("reject", getattr(e, "code", type(e).__name__)))

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(call, range(16)))
    wins = [o for o in outcomes if o[0] == "ok"]
    rejects = [o for o in outcomes if o[0] == "reject"]
    assert len(wins) == 1 and wins[0][1] == "completed", wins
    assert all(o[1] == "release_finished" for o in rejects), rejects
    history = [h for h in service.history() if h.release_id == r.release_id]
    assert len(history) == 1 and history[0].final_status == ReleaseStatus.COMPLETED
    print(f"OUTCOMES: exactly one completion + {len(rejects)} release_finished")


def test_concurrent_rollback_same_request_id_single_history_entry(service):
    r = _ready_release(service, failed=0)
    print(f"\nINPUT: 24 concurrent rollback(request_id='rb')")
    barrier = threading.Barrier(24)

    def call():
        barrier.wait()
        return service.rollback(r.release_id, reason="manual", request_id="rb")

    with ThreadPoolExecutor(max_workers=24) as pool:
        results = list(pool.map(lambda _: call(), range(24)))
    rolled = [x for x in results if not x.idempotent_hit]
    replays = [x for x in results if x.idempotent_hit]
    assert len(rolled) == 1 and len(replays) == 23
    assert all(x.to_status == ReleaseStatus.ROLLED_BACK for x in results)
    history = [h for h in service.history() if h.release_id == r.release_id]
    assert len(history) == 1  # no double rollback
    print(f"TRANSITION: one real rollback + 23 replays; history entries={len(history)}")


def test_concurrent_mixed_promote_and_rollback(service):
    # Unhealthy window: promote must auto-rollback; concurrent rollback must
    # never produce a second conclusion or a contradictory COMPLETED state.
    r = _ready_release(service, failed=10)  # fail_rate 0.10 >= 0.05
    print(f"\nINPUT: 12 promote + 12 rollback concurrently on unhealthy release")
    barrier = threading.Barrier(24)
    final_states, errors = [], []
    lock = threading.Lock()

    def call(kind, i):
        barrier.wait()
        try:
            if kind == "p":
                res = service.promote(r.release_id, request_id=f"p{i}")
                with lock:
                    final_states.append(("promote", res.to_status.value, res.rolled_back))
            else:
                res = service.rollback(r.release_id, reason="manual", request_id=f"r{i}")
                with lock:
                    final_states.append(("rollback", res.to_status.value, False))
        except ReleaseFinishedError as e:
            with lock:
                errors.append(e.code)

    with ThreadPoolExecutor(max_workers=24) as pool:
        futs = [pool.submit(call, "p", i) for i in range(12)]
        futs += [pool.submit(call, "r", i) for i in range(12)]
        [f.result() for f in futs]
    assert service.get_release(r.release_id).status == ReleaseStatus.ROLLED_BACK
    history = [h for h in service.history() if h.release_id == r.release_id]
    assert len(history) == 1, history
    assert set(errors) <= {"release_finished"}
    print(f"CONCLUSION: rolled_back once; losers saw release_finished: {errors}")
    print(f"winning calls: {final_states}")
    assert all(s[1] == "rolled_back" for s in final_states)


def test_replaying_completed_promote_keeps_completed(service):
    r = _ready_release(service, failed=0)
    first = service.promote(r.release_id, request_id="step1")
    assert first.to_status == ReleaseStatus.CANARY
    # feed step-1 window and finish with a fixed request id
    for m in range(2):
        service.observe(r.release_id, 1, ObservationPoint(m, 100, 0))
    # step 2 (100%) ...
    s2 = service.promote(r.release_id, request_id="step2")
    assert s2.new_version_percent == 100
    for m in range(2):
        service.observe(r.release_id, 2, ObservationPoint(m, 100, 0))
    done = service.promote(r.release_id, request_id="finish")
    assert done.to_status == ReleaseStatus.COMPLETED
    replay = service.promote(r.release_id, request_id="finish")
    assert replay.to_status == ReleaseStatus.COMPLETED and replay.rolled_back is False
    print("\nINPUT: replay finishing request on COMPLETED -> same completed conclusion")
    # a *new* request on the finished release is rejected, not replayed
    with pytest.raises(ReleaseFinishedError):
        service.promote(r.release_id, request_id="brand-new")
    print("REJECT: new request on terminal release -> release_finished (distinct from replay)")


def test_same_request_id_different_action_rejected(service):
    r = _ready_release(service, failed=0)
    service.promote(r.release_id, request_id="shared")
    with pytest.raises(IllegalStateError) as ei:
        service.rollback(r.release_id, request_id="shared")
    assert ei.value.details["original_action"] == "promote"
    print(f"\nREJECT: reused request_id for different action -> {ei.value.code}")


def test_step_skipping_under_concurrency(service):
    r = _ready_release(service, failed=0)
    print("\nINPUT: concurrent promote naming step 5 -> every call rejected")
    barrier = threading.Barrier(8)

    def call(i):
        barrier.wait()
        try:
            service.promote(r.release_id, step_index=5, request_id=f"bad{i}")
            return "unexpected-ok"
        except StepSkippingError:
            return "skip"
        except Exception as e:  # pragma: no cover - must not happen
            return type(e).__name__

    with ThreadPoolExecutor(max_workers=8) as pool:
        out = list(pool.map(call, range(8)))
    assert set(out) == {"skip"}, out
    assert service.get_release(r.release_id).current_step_index == 0
    print("all 8 calls -> step_skipping; state untouched")
