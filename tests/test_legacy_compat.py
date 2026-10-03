"""Legacy local state-file compatibility tests.

Three business areas, pinned against state files written by older versions:

1. records missing fields (``policy_version`` / ``steps`` / sparse scalars) —
   one sparse old record must not abort loading the whole file;
2. releases referencing a policy whose definition was never stored — they keep
   running under the fixed default threshold / sample / duration semantics;
3. restart + repeated evaluation stability — the same stored record produces
   identical judgements in fresh processes, and finished conclusions/history
   are never rewritten or duplicated.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests._mp_worker_test_support import run_worker

# Fixed legacy defaults — duplicated intentionally here so the tests pin the
# numeric contract rather than import the implementation constants.
LEGACY_THRESHOLD = 0.05
LEGACY_MIN_REQUESTS = 100
LEGACY_OBSERVE_MINUTES = 5
LEGACY_STEPS = [10, 30, 50, 100]


def _write_state(tmp_path: Path, payload: dict, *, name: str = "state.json") -> str:
    path = tmp_path / name
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
    return str(path)


def _default_policy_dict(version: int = 1, *, steps=None) -> dict:
    steps = LEGACY_STEPS if steps is None else steps
    return {
        "version": version,
        "name": "default",
        "failure_rate_threshold": LEGACY_THRESHOLD,
        "min_requests": LEGACY_MIN_REQUESTS,
        "created_at": "",
        "steps": [
            {"new_version_percent": p, "observe_minutes": LEGACY_OBSERVE_MINUTES}
            for p in steps
        ],
    }


def _legacy_release(
    *,
    release_id: str = "rel-old",
    service: str = "legacy-svc",
    environment: str = "prod",
    status: str = "pending",
    policy_version=1,
    steps=None,
    current_step_index: int = -1,
    include_policy_version: bool = True,
    include_steps: bool = True,
    observations=None,
) -> dict:
    if steps is None:
        steps = [
            {"new_version_percent": p} for p in LEGACY_STEPS
        ]  # no observe_minutes: legacy shape
    raw = {
        "release_id": release_id,
        "service": service,
        "environment": environment,
        "old_version": "v1",
        "new_version": "v2",
        "status": status,
        "current_step_index": current_step_index,
    }
    if include_policy_version:
        raw["policy_version"] = policy_version
    if include_steps:
        raw["steps"] = steps
    if observations is not None:
        raw["observations"] = observations
    return raw


class TestMissingFields:
    """Business: sparse records written before policy_version/steps existed."""

    def test_missing_policy_version_loads_with_default(self, tmp_path):
        print("\nCASE missing_fields/missing_policy_version: loads as v1, proceeds")
        state = _write_state(
            tmp_path,
            {
                "releases": [
                    _legacy_release(release_id="rel-npv", include_policy_version=False)
                ],
                "decisions": [],
                "history": [],
                "policies": [_default_policy_dict(1)],
            },
        )
        rel = run_worker(state, "get_release", {"release_id": "rel-npv"})
        assert rel["ok"] is True
        assert rel["policy_version"] == 1
        assert rel["observe_minutes"] == [5, 5, 5, 5]
        print("  missing policy_version -> bound to default v1, durations 5")

        enter = run_worker(
            state, "promote", {"release_id": "rel-npv", "kwargs": {"request_id": "e"}}
        )
        assert enter["to_status"] == "canary"
        assert enter["new_version_percent"] == 10
        print("  release enters step0 @10% under default policy, no crash")

    def test_missing_steps_and_scalars_load_with_defaults(self, tmp_path):
        print("\nCASE missing_fields/missing_steps: default ramp and empty scalars")
        state = _write_state(
            tmp_path,
            {
                "releases": [
                    _legacy_release(release_id="rel-nosteps", include_steps=False)
                ],
                "decisions": [],
                "history": [],
                "policies": [_default_policy_dict(1)],
            },
        )
        rel = run_worker(state, "get_release", {"release_id": "rel-nosteps"})
        assert rel["steps"] == LEGACY_STEPS
        assert rel["observe_minutes"] == [5, 5, 5, 5]
        print("  missing steps -> [10,30,50,100], each observe_minutes=5")

    def test_legacy_bare_int_steps_get_default_observe_minutes(self, tmp_path):
        print("\nCASE missing_fields/bare_int_steps: [10,100] -> 5min per step")
        state = _write_state(
            tmp_path,
            {
                "releases": [_legacy_release(release_id="rel-bare", steps=[10, 100])],
                "decisions": [],
                "history": [],
                "policies": [_default_policy_dict(1, steps=[10, 100])],
            },
        )
        rel = run_worker(state, "get_release", {"release_id": "rel-bare"})
        assert rel["steps"] == [10, 100]
        assert rel["observe_minutes"] == [5, 5]
        print("  bare-int steps coerced to (percent, observe_minutes=5)")

    def test_one_bad_record_does_not_poison_other_records(self, tmp_path):
        print("\nCASE missing_fields/sparse_record_isolation: good record usable")
        good = _legacy_release(
            release_id="rel-good",
            service="svc-b",
            policy_version=1,
            steps=[
                {"new_version_percent": 10, "observe_minutes": 5},
                {"new_version_percent": 100, "observe_minutes": 5},
            ],
        )
        sparse = _legacy_release(
            release_id="rel-sparse",
            service="svc-a",
            include_policy_version=False,
            steps=[{"new_version_percent": 10}, {"new_version_percent": 100}],
        )
        state = _write_state(
            tmp_path,
            {
                "releases": [sparse, good],
                "decisions": [],
                "history": [
                    # history record from the same era: sparse scalar fields
                    {
                        "release_id": "h-old",
                        "service": "svc-a",
                        "environment": "staging",
                        "old_version": "v0",
                        "new_version": "v1",
                        "final_status": "completed",
                    },
                ],
                "policies": [
                    _default_policy_dict(
                        1,
                        steps=[
                            {"new_version_percent": 10, "observe_minutes": 5},
                            {"new_version_percent": 100, "observe_minutes": 5},
                        ],
                    )
                ],
            },
        )
        # Every record in the file must be reachable; the file must open at all.
        for rid in ("rel-sparse", "rel-good"):
            rel = run_worker(state, "get_release", {"release_id": rid})
            assert rel["ok"] is True, rel
        good_after = run_worker(
            state, "promote", {"release_id": "rel-good", "kwargs": {"request_id": "go"}}
        )
        assert good_after["to_status"] == "canary"
        hist = run_worker(state, "history", {})["entries"]
        old = next(h for h in hist if h["release_id"] == "h-old")
        assert old["final_status"] == "completed"
        assert old["reason"] == "" and old["policy_snapshot"] == {}
        print("  sparse + good + sparse history all load; good release advances")


class TestUndefinedPolicyReference:
    """Business: releases bound to a policy version absent from the file."""

    @staticmethod
    def _ghost_state(
        tmp_path,
        *,
        policy_version=7,
        release_id="rel-ghost",
        status="canary",
        current_step_index=0,
        ramp=((20, 3), (60, 3), (100, 3)),
        policies=None,
    ):
        steps = [{"new_version_percent": p, "observe_minutes": m} for p, m in ramp]
        return _write_state(
            tmp_path,
            {
                "releases": [
                    _legacy_release(
                        release_id=release_id,
                        service="billing",
                        status=status,
                        policy_version=policy_version,
                        steps=steps,
                        current_step_index=current_step_index,
                    )
                ],
                "decisions": [],
                "history": [],
                # Only default v1 exists; the release's referenced version is gone.
                "policies": [_default_policy_dict(1)] if policies is None else policies,
            },
        )

    def test_ghost_policy_evaluates_with_fixed_defaults(self, tmp_path):
        print("\nCASE ghost_policy/evaluate: no policy_not_found; fixed defaults")
        state = self._ghost_state(tmp_path, policy_version=7)
        # Empty window -> pending, but the details already expose the default
        # semantics: 3-minute window comes from the release steps, min 100
        # samples / 0.05 threshold come from the fixed legacy defaults.
        pending = run_worker(state, "evaluate", {"release_id": "rel-ghost"})
        assert pending["ok"] is False
        assert pending["code"] == "observation_pending"
        assert pending["details"]["required_minutes"] == 3
        assert pending["details"]["min_requests"] == LEGACY_MIN_REQUESTS
        print("  evaluate works; required_minutes=3 (stored), min_requests=100")

        # Complete the window at exactly 95/100 = 0.05 fail rate: the fixed
        # default threshold 0.05 with boundary-inclusive '>=' must roll back.
        for minute in range(3):
            run_worker(
                state,
                "observe",
                {
                    "release_id": "rel-ghost",
                    "step_index": 0,
                    "minute_offset": minute,
                    "total_requests": 100 if minute == 0 else 0,
                    "failed_requests": 5 if minute == 0 else 0,
                },
            )
        rb = run_worker(
            state, "promote", {"release_id": "rel-ghost", "kwargs": {"request_id": "j"}}
        )
        assert rb["rolled_back"] is True
        assert rb["to_status"] == "rolled_back"
        print("  fail_rate 5/100 == default threshold 0.05 -> boundary rollback")

    def test_ghost_policy_release_can_complete_after_restart(self, tmp_path):
        print("\nCASE ghost_policy/complete_after_restart: healthy path proceeds")
        state = self._ghost_state(tmp_path, policy_version=3)
        # Feed every window fully across 3 steps, every action a fresh process
        # (i.e. a "restart"). 0 failures / 100 samples -> healthy each time.
        for step in range(3):
            for minute in range(3):
                run_worker(
                    state,
                    "observe",
                    {
                        "release_id": "rel-ghost",
                        "step_index": step,
                        "minute_offset": minute,
                        "total_requests": 100 if minute == 0 else 0,
                        "failed_requests": 0,
                    },
                )
            result = run_worker(
                state,
                "promote",
                {"release_id": "rel-ghost", "kwargs": {"request_id": f"p{step}"}},
            )
            if step < 2:
                assert result["to_status"] == "canary"
                assert result["new_version_percent"] == (60, 100)[step]
        assert result["to_status"] == "completed"
        print("  ghost release ramps 20% -> 60% -> 100% -> completed")

        hist = run_worker(state, "history", {})["entries"]
        assert len(hist) == 1 and hist[0]["final_status"] == "completed"
        snap = hist[0]["policy_snapshot"]
        assert snap["version"] == 3
        assert snap["failure_rate_threshold"] == LEGACY_THRESHOLD
        assert snap["min_requests"] == LEGACY_MIN_REQUESTS
        assert [
            (s["new_version_percent"], s["observe_minutes"]) for s in snap["steps"]
        ] == [(20, 3), (60, 3), (100, 3)]
        print("  history snapshot keeps version 3 + fixed defaults + own steps")

    def test_ghost_release_rollback_writes_exactly_one_history(self, tmp_path):
        print("\nCASE ghost_policy/single_history: rollback then restart/replay")
        state = self._ghost_state(tmp_path, policy_version=9)
        rb = run_worker(
            state,
            "rollback",
            {"release_id": "rel-ghost", "kwargs": {"request_id": "rb"}},
        )
        assert rb["to_status"] == "rolled_back"
        # A fresh process replays the same idempotent conclusion and does not
        # append another history entry.
        replay = run_worker(
            state,
            "rollback",
            {"release_id": "rel-ghost", "kwargs": {"request_id": "rb"}},
        )
        assert replay["to_status"] == "rolled_back"
        again = run_worker(state, "rollback", {"release_id": "rel-ghost"})
        assert again["ok"] is False and again["code"] == "release_finished"
        hist = run_worker(state, "history", {})["entries"]
        assert len(hist) == 1 and hist[0]["final_status"] == "rolled_back"
        assert hist[0]["policy_snapshot"]["version"] == 9
        print("  one rolled_back history after rollback + replay + new request")

    def test_new_policy_creation_skips_occupied_ghost_version(self, tmp_path):
        print("\nCASE ghost_policy/version_allocation: ghost v3 never hijacked")
        state = self._ghost_state(
            tmp_path, policy_version=3, status="pending", current_step_index=-1
        )
        # Version allocation is high-water over both stored definitions and
        # ghost references: the next free version is max(1, 3) + 1 = 4 (a gap
        # at 2 is possible; what must never happen is taking over version 3).
        v4 = run_worker(
            state,
            "create_policy",
            {
                "name": "strict",
                "steps": [10, 50, 100],
                "failure_rate_threshold": 0.01,
                "min_requests": 500,
                "observe_minutes": 2,
            },
        )
        assert v4["version"] == 4
        v5 = run_worker(state, "create_policy", {"name": "other", "steps": [10, 100]})
        assert v5["version"] == 5
        versions = run_worker(state, "list_policies")["versions"]
        assert versions == [1, 4, 5]
        print(f"  allocated versions {versions}; ghost v3 left occupied")

        # The ghost release still evaluates under fixed defaults — a full
        # healthy window under default 100 samples advances.
        run_worker(
            state, "promote", {"release_id": "rel-ghost", "kwargs": {"request_id": "e"}}
        )
        for minute in range(3):
            run_worker(
                state,
                "observe",
                {
                    "release_id": "rel-ghost",
                    "step_index": 0,
                    "minute_offset": minute,
                    "total_requests": 100 if minute == 0 else 0,
                    "failed_requests": 2 if minute == 0 else 0,
                },
            )  # 2% healthy for v1
        adv = run_worker(
            state,
            "promote",
            {"release_id": "rel-ghost", "kwargs": {"request_id": "adv"}},
        )
        assert adv["to_status"] == "canary" and adv["step_index"] == 1
        print("  ghost release still healthy at 2% (would be unhealthy under v2)")


class TestRestartDeterminism:
    """Business: stable defaults across repeated restarts/evaluations."""

    @staticmethod
    def _mid_window_ghost(
        tmp_path, *, release_id="rel-det", policy_version=5, failed=4
    ):
        # canary @ step0, 2 of 3 minutes recorded, 4% fail rate of 100 samples
        observations = {
            "0": [
                {"minute_offset": 0, "total_requests": 100, "failed_requests": failed},
                {"minute_offset": 1, "total_requests": 0, "failed_requests": 0},
            ]
        }
        return _write_state(
            tmp_path,
            {
                "releases": [
                    _legacy_release(
                        release_id=release_id,
                        service="billing",
                        status="canary",
                        policy_version=policy_version,
                        steps=[
                            {"new_version_percent": 20, "observe_minutes": 3},
                            {"new_version_percent": 100, "observe_minutes": 3},
                        ],
                        current_step_index=0,
                        observations=observations,
                    )
                ],
                "decisions": [],
                "history": [],
                "policies": [_default_policy_dict(1)],
            },
        )

    def test_repeated_restarts_and_evaluations_are_identical(self, tmp_path):
        print("\nCASE restart_determinism/repeated_evaluate: same verdict x N")
        state = self._mid_window_ghost(tmp_path)
        # 5 fresh processes, each only evaluating: window incomplete, every
        # single one must report the exact same governing parameters.
        for restart in range(5):
            pending = run_worker(state, "evaluate", {"release_id": "rel-det"})
            assert pending["code"] == "observation_pending"
            d = pending["details"]
            assert (
                d["required_minutes"],
                d["min_requests"],
                d["observed_minutes"],
                d["total_requests"],
            ) == (3, 100, 2, 100)
        print("  5 restarts x evaluate: required=3 min, min_requests=100, 2 mins")

        # Finish the window in a 6th process: 4% < 5% default -> healthy.
        run_worker(
            state,
            "observe",
            {
                "release_id": "rel-det",
                "step_index": 0,
                "minute_offset": 2,
                "total_requests": 0,
                "failed_requests": 0,
            },
        )
        adv = run_worker(
            state, "promote", {"release_id": "rel-det", "kwargs": {"request_id": "a"}}
        )
        assert adv["to_status"] == "canary" and adv["step_index"] == 1
        print("  after restart: 4% fail rate healthy vs fixed 5% threshold")

        # Boundary stability across restarts: 5% at step1 (5 samples of 100)
        # rolls back under the >= rule; repeat the promote with the same
        # request_id in fresh processes -> identical replay, no second history.
        for minute in range(3):
            run_worker(
                state,
                "observe",
                {
                    "release_id": "rel-det",
                    "step_index": 1,
                    "minute_offset": minute,
                    "total_requests": 100 if minute == 0 else 0,
                    "failed_requests": 5 if minute == 0 else 0,
                },
            )
        for restart in range(3):
            result = run_worker(
                state,
                "promote",
                {"release_id": "rel-det", "kwargs": {"request_id": "rb-judge"}},
            )
            assert result["rolled_back"] is True
            assert result["to_status"] == "rolled_back"
        hist = run_worker(state, "history", {})["entries"]
        assert len(hist) == 1
        assert hist[0]["policy_snapshot"]["failure_rate_threshold"] == 0.05
        print("  boundary 5% -> rollback; 3 replays stay identical, 1 history")

    def test_finished_legacy_conclusion_never_rewritten(self, tmp_path):
        print("\nCASE restart_determinism/finished_immutable: status+history fixed")
        # A release already finished in the legacy file, with a history entry
        # carrying the policy snapshot as written then. Nothing may rewrite it.
        finished = _legacy_release(
            release_id="rel-done",
            service="billing",
            status="completed",
            policy_version=4,
            steps=[{"new_version_percent": 100, "observe_minutes": 7}],
            current_step_index=0,
        )
        finished["finish_reason"] = "final observation window healthy"
        history_entry = {
            "release_id": "rel-done",
            "service": "billing",
            "environment": "prod",
            "old_version": "v1",
            "new_version": "v2",
            "final_status": "completed",
            "reason": "final observation window healthy",
            "policy_snapshot": {
                "version": 4,
                "name": "ancient-custom",
                "failure_rate_threshold": 0.42,
                "min_requests": 777,
                "created_at": "2020-01-01T00:00:00+00:00",
                "steps": [{"new_version_percent": 100, "observe_minutes": 7}],
            },
            "finished_at": "2020-01-01T00:00:00+00:00",
        }
        state = _write_state(
            tmp_path,
            {
                "releases": [finished],
                "decisions": [],
                "history": [history_entry],
                "policies": [_default_policy_dict(1)],
            },
        )

        original_bytes = Path(state).read_bytes()
        for restart in range(3):
            rel = run_worker(state, "get_release", {"release_id": "rel-done"})
            assert rel["status"] == "completed"
            hist = run_worker(state, "history", {})["entries"]
            assert len(hist) == 1
            assert hist[0]["policy_snapshot"]["failure_rate_threshold"] == 0.42
            assert hist[0]["policy_snapshot"]["min_requests"] == 777
            # Any attempt to keep pushing is rejected as already finished.
            rejected = run_worker(
                state, "promote", {"release_id": "rel-done", "kwargs": {}}
            )
            assert rejected["ok"] is False
            assert rejected["code"] == "release_finished"
        # Pure reads never rewrite the file: its bytes stay untouched.
        assert Path(state).read_bytes() == original_bytes
        print("  3 restart/read cycles: snapshot 0.42/777 intact, file untouched")

    def test_synthetic_default_is_never_persisted(self, tmp_path):
        print("\nCASE restart_determinism/no_persistence: fallback is in-memory")
        state = self._mid_window_ghost(tmp_path, policy_version=8)
        # Reading and evaluating (pending) against the missing v8 must not add a
        # v8 definition to the policies section.
        run_worker(state, "get_release", {"release_id": "rel-det"})
        run_worker(state, "evaluate", {"release_id": "rel-det"})
        versions = run_worker(state, "list_policies")["versions"]
        assert versions == [1]
        with open(state, encoding="utf-8") as fh:
            on_disk = json.load(fh)
        assert [p["version"] for p in on_disk["policies"]] == [1]
        print("  ghost v8 evaluate/read leaves policies section as [v1] only")

        # Finish the release: the history snapshot still says version 8, while
        # the policies section remains unchanged (synthesis never backfilled).
        run_worker(
            state,
            "observe",
            {
                "release_id": "rel-det",
                "step_index": 0,
                "minute_offset": 2,
                "total_requests": 0,
                "failed_requests": 0,
            },
        )
        rb = run_worker(
            state, "rollback", {"release_id": "rel-det", "kwargs": {"request_id": "r"}}
        )
        assert rb["to_status"] == "rolled_back"
        with open(state, encoding="utf-8") as fh:
            on_disk = json.load(fh)
        assert [p["version"] for p in on_disk["policies"]] == [1]
        snap = on_disk["history"][0]["policy_snapshot"]
        assert snap["version"] == 8
        assert snap["failure_rate_threshold"] == 0.05
        print("  rollback writes history snapshot v8/defaults; no v8 policy row")
