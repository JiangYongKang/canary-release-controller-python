"""Canary ramp engine: state machine, health/rollback judgements.

Concurrency & idempotency
-------------------------
Every mutating call executes inside the store's re-entrant lock as one atomic
read-modify-write transaction, so concurrent promote/rollback submissions
cannot interleave into a wrong conclusion.  A decision ledger keyed by
``(release_id, request_id)`` makes retries idempotent:

* same request_id + **same action** -> previous result replayed;
* same request_id + **different action** -> ``illegal_state`` rejected;
* no request_id / new request_id on a terminal release ->
  ``release_finished`` rejected (distinguishable from idempotent replay).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Callable

from canary.errors import (
    IllegalStateError,
    InvalidArgumentError,
    ObservationPendingError,
    ReleaseAlreadyExistsError,
    ReleaseFinishedError,
    ReleaseNotFoundError,
    StepSkippingError,
    UnhealthyRollbackError,
)
from canary.models import (
    CanaryStep,
    HistoryEntry,
    ObservationPoint,
    PromotionResult,
    Release,
    ReleaseStatus,
    RollbackResult,
)
from canary.policy import PolicyRegistry, PromotionPolicy
from canary.store import LocalStore

logger = logging.getLogger("canary.engine")

_TERMINAL = (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CanaryEngine:
    def __init__(
        self,
        store: LocalStore,
        policies: PolicyRegistry,
        clock: Callable[[], str] = _utc_now,
    ) -> None:
        self.store = store
        self.policies = policies
        self._clock = clock

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #
    def register_release(
        self,
        *,
        service: str,
        environment: str,
        old_version: str,
        new_version: str,
        policy_version: int | None = None,
    ) -> Release:
        if not all([service, environment, old_version, new_version]):
            raise InvalidArgumentError(
                "invalid_argument",
                details={"field": "service/environment/versions", "must": "non-empty"},
            )
        if old_version == new_version:
            raise InvalidArgumentError(
                "invalid_argument",
                details={"field": "new_version", "problem": "must differ from old_version"},
            )
        with self.store.lock():
            existing = self.store.find_release(service, environment)
            if existing is not None:
                raise ReleaseAlreadyExistsError(
                    "release_already_exists",
                    details={"existing_release_id": existing.release_id},
                )
            policy = (
                self.policies.get(policy_version)
                if policy_version is not None
                else self.policies.latest()
            )
            release = Release(
                release_id=f"rel-{uuid.uuid4().hex[:12]}",
                service=service,
                environment=environment,
                old_version=old_version,
                new_version=new_version,
                status=ReleaseStatus.PENDING,
                policy_version=policy.version,
                steps=list(policy.steps),
                current_step_index=-1,
                created_at=self._clock(),
                updated_at=self._clock(),
            )
            self.store.put_release(release)
            logger.info(
                "register input=%s",
                {
                    "service": service,
                    "environment": environment,
                    "old": old_version,
                    "new": new_version,
                    "policy_version": policy.version,
                },
            )
            logger.info(
                "transition %s: <none> -> %s", release.release_id,
                ReleaseStatus.PENDING.value,
            )
            return release

    # ------------------------------------------------------------------ #
    # Observations
    # ------------------------------------------------------------------ #
    def record_observation(
        self,
        release_id: str,
        step_index: int,
        point: ObservationPoint,
    ) -> None:
        if point.total_requests < 0 or point.failed_requests < 0:
            raise InvalidArgumentError(
                "invalid_argument", details={"field": "requests", "must": ">= 0"}
            )
        if point.failed_requests > point.total_requests:
            raise InvalidArgumentError(
                "invalid_argument",
                details={"field": "failed_requests", "problem": "> total_requests"},
            )
        if point.minute_offset < 0:
            raise InvalidArgumentError(
                "invalid_argument", details={"field": "minute_offset", "must": ">= 0"}
            )
        with self.store.lock():
            release = self._require_release(release_id)
            self._require_active(release)
            if not (0 <= step_index < len(release.steps)):
                raise InvalidArgumentError(
                    "invalid_argument",
                    details={"field": "step_index", "valid": f"0..{len(release.steps)-1}"},
                )
            if step_index > release.current_step_index:
                raise IllegalStateError(
                    "illegal_state",
                    details={
                        "problem": "cannot record observation for a step not entered",
                        "step_index": step_index,
                        "current_step_index": release.current_step_index,
                    },
                )
            bucket = release.observations.setdefault(step_index, [])
            if any(p.minute_offset == point.minute_offset for p in bucket):
                raise IllegalStateError(
                    "illegal_state",
                    details={
                        "problem": "duplicate observation at same minute_offset",
                        "step_index": step_index,
                        "minute_offset": point.minute_offset,
                    },
                )
            bucket.append(point)
            bucket.sort(key=lambda p: p.minute_offset)
            release.updated_at = self._clock()
            self.store.put_release(release)
            logger.info(
                "observe input=%s",
                {
                    "release_id": release_id,
                    "step": step_index,
                    "minute": point.minute_offset,
                    "total": point.total_requests,
                    "failed": point.failed_requests,
                },
            )

    # ------------------------------------------------------------------ #
    # Health judgement
    # ------------------------------------------------------------------ #
    def evaluate(self, release_id: str) -> tuple[bool, str]:
        """Evaluate the *current* step's observation window.

        Returns ``(healthy, reason)``.  Raises
        :class:`ObservationPendingError` while the window is not yet full or
        has not collected ``min_requests`` samples — an incomplete window can
        never cause a rollback judgement.
        """
        with self.store.lock():
            release = self._require_release(release_id)
            policy = self._policy_for(release)
            return self._evaluate_locked(release, policy)

    def _evaluate_locked(
        self, release: Release, policy: PromotionPolicy
    ) -> tuple[bool, str]:
        idx = release.current_step_index
        if idx < 0:
            raise ObservationPendingError(
                "observation_pending",
                details={"problem": "release has not entered any canary step"},
            )
        step = release.steps[idx]
        points = release.observations.get(idx, [])
        required_minutes = step.observe_minutes
        total = sum(p.total_requests for p in points)
        failed = sum(p.failed_requests for p in points)

        # Observation window fullness: distinct observed minutes must cover the
        # whole configured window (minute offsets 0..observe_minutes-1).
        covered = {p.minute_offset for p in points}
        window_full = required_minutes <= len(covered) and covered == set(
            range(required_minutes)
        )
        enough_samples = total >= policy.min_requests
        logger.info(
            "evaluate input=%s",
            {
                "release_id": release.release_id,
                "step": idx,
                "points": len(points),
                "required_minutes": required_minutes,
                "covered_minutes": sorted(covered),
                "total": total,
                "failed": failed,
                "min_requests": policy.min_requests,
            },
        )
        if not window_full or not enough_samples:
            reason = (
                f"window incomplete (window_full={window_full},"
                f" enough_samples={enough_samples})"
            )
            logger.info("judgement=pending reason=%s", reason)
            raise ObservationPendingError(
                "observation_pending",
                details={
                    "window_full": window_full,
                    "enough_samples": enough_samples,
                    "required_minutes": required_minutes,
                    "observed_minutes": len(covered),
                    "total_requests": total,
                    "min_requests": policy.min_requests,
                },
            )
        fail_rate = failed / total if total else 0.0
        # Threshold boundary: fail_rate >= threshold => unhealthy.
        healthy = fail_rate < policy.failure_rate_threshold
        reason = (
            f"fail_rate={fail_rate:.6f} threshold={policy.failure_rate_threshold}"
            f" (failed={failed}, total={total}); rule: fail_rate >= threshold -> rollback"
        )
        logger.info("judgement=%s reason=%s", "healthy" if healthy else "unhealthy", reason)
        return healthy, reason

    # ------------------------------------------------------------------ #
    # Promotion
    # ------------------------------------------------------------------ #
    def promote(
        self,
        release_id: str,
        *,
        step_index: int | None = None,
        request_id: str = "",
    ) -> PromotionResult:
        with self.store.lock():
            # Idempotency replay must take priority over state checks.
            replay = self.store.decision_seen(release_id, request_id)
            if replay is not None:
                return self._replay_decision(
                    release_id, request_id, replay, expect_action="promote"
                )
            release = self._require_release(release_id)
            from_status = release.status

            if release.status in _TERMINAL:
                raise ReleaseFinishedError(
                    "release_finished",
                    details={"status": release.status.value},
                )
            if release.status == ReleaseStatus.PENDING:
                # Entering the first step: no prior window to evaluate and the
                # requested target must be exactly step 0.
                target = self._resolve_target_step(release, step_index)
                commit = "step"
            else:
                # CANARY: the current window must be evaluated before anything
                # else.  A failed window auto-rolls back; a healthy window at
                # the last step completes the release, otherwise the caller may
                # (redundantly) name the next step but simply asking to promote
                # is enough — skipping is still rejected.
                at_last = release.current_step_index == len(release.steps) - 1
                if at_last:
                    if step_index not in (None, len(release.steps)):
                        raise StepSkippingError(
                            "step_skipping",
                            details={
                                "requested_step": step_index,
                                "expected_next_step": len(release.steps),
                                "note": "final step passed; omit step_index to complete",
                            },
                        )
                else:
                    # Deterministic input validation first: an illegal target is
                    # rejected regardless of window state.
                    self._resolve_target_step(release, step_index)
                policy = self._policy_for(release)
                healthy, reason = self._evaluate_locked(release, policy)
                if not healthy:
                    rb = self._do_rollback_locked(release, "auto", reason)
                    result = PromotionResult(
                        release_id=release.release_id,
                        from_status=from_status,
                        to_status=rb.to_status,
                        step_index=release.current_step_index,
                        new_version_percent=0,
                        evaluated=True,
                        rolled_back=True,
                        reason=f"auto-rollback: {reason}",
                    )
                    self._remember(request_id, "promote", result)
                    logger.info(
                        "transition %s: %s -> %s (auto rollback during promote)",
                        release.release_id, from_status.value, rb.to_status.value,
                    )
                    return result
                if at_last:
                    commit = "complete"
                    target = release.current_step_index
                else:
                    target = self._resolve_target_step(release, step_index)
                    commit = "step"

            # Commit either a step entry or the healthy completion.
            previous_index = release.current_step_index
            if commit == "complete":
                release.status = ReleaseStatus.COMPLETED
                release.finish_reason = (
                    "final observation window healthy; new version at 100%"
                )
                to_status = ReleaseStatus.COMPLETED
                percent = release.steps[target].new_version_percent
                self._finalize_history(release)
                reason_text = f"completed after healthy step {target} @ {percent}%"
            else:
                release.current_step_index = target
                percent = release.steps[target].new_version_percent
                release.status = ReleaseStatus.CANARY
                to_status = ReleaseStatus.CANARY
                reason_text = (
                    f"advanced step {previous_index} -> {target},"
                    f" new_version_percent={percent}"
                )
            release.updated_at = self._clock()
            self.store.put_release(release)
            result = PromotionResult(
                release_id=release.release_id,
                from_status=from_status,
                to_status=to_status,
                step_index=target,
                new_version_percent=percent,
                evaluated=from_status == ReleaseStatus.CANARY,
                rolled_back=False,
                reason=reason_text,
            )
            self._remember(request_id, "promote", result)
            logger.info(
                "transition %s: %s -> %s step=%s percent=%s",
                release.release_id, from_status.value, to_status.value, target, percent,
            )
            return result

    def _resolve_target_step(self, release: Release, requested: int | None) -> int:
        expected = release.current_step_index + 1
        if requested is None:
            return expected
        if requested != expected:
            raise StepSkippingError(
                "step_skipping",
                details={
                    "requested_step": requested,
                    "expected_next_step": expected,
                    "current_step_index": release.current_step_index,
                },
            )
        return expected

    # ------------------------------------------------------------------ #
    # Rollback
    # ------------------------------------------------------------------ #
    def rollback(
        self,
        release_id: str,
        *,
        reason: str = "manual",
        request_id: str = "",
    ) -> RollbackResult:
        with self.store.lock():
            replay = self.store.decision_seen(release_id, request_id)
            if replay is not None:
                return self._replay_decision(
                    release_id, request_id, replay, expect_action="rollback"
                )
            release = self._require_release(release_id)
            if release.status == ReleaseStatus.ROLLED_BACK:
                raise ReleaseFinishedError(
                    "release_finished", details={"status": "rolled_back"}
                )
            if release.status == ReleaseStatus.COMPLETED:
                raise ReleaseFinishedError(
                    "release_finished", details={"status": "completed"}
                )
            from_status = release.status
            result = self._do_rollback_locked(release, reason, reason)
            result.from_status = from_status
            self._remember(request_id, "rollback", result)
            logger.info(
                "transition %s: %s -> rolled_back trigger=%s",
                release.release_id, from_status.value, reason,
            )
            return result

    def _do_rollback_locked(
        self, release: Release, trigger: str, reason: str
    ) -> RollbackResult:
        previous = release.status
        release.status = ReleaseStatus.ROLLED_BACK
        release.finish_reason = f"{trigger}: {reason}"
        release.updated_at = self._clock()
        self.store.put_release(release)
        self._finalize_history(release)
        return RollbackResult(
            release_id=release.release_id,
            from_status=previous,
            to_status=ReleaseStatus.ROLLED_BACK,
            trigger=trigger,
            reason=reason,
            idempotent_hit=False,
        )

    # ------------------------------------------------------------------ #
    # History / helpers
    # ------------------------------------------------------------------ #
    def _finalize_history(self, release: Release) -> None:
        policy = self._policy_for(release)
        entry = HistoryEntry(
            release_id=release.release_id,
            service=release.service,
            environment=release.environment,
            old_version=release.old_version,
            new_version=release.new_version,
            final_status=release.status,
            reason=release.finish_reason,
            policy_snapshot=policy.to_dict(),
            finished_at=self._clock(),
        )
        self.store.append_history(entry)
        logger.info(
            "history finalized release=%s conclusion=%s",
            release.release_id, release.status.value,
        )

    def get_release(self, release_id: str) -> Release:
        with self.store.lock():
            return self._require_release(release_id)

    def _policy_for(self, release: Release) -> PromotionPolicy:
        """Resolve the policy a stored release is bound to.

        Legacy state files may reference a policy version whose definition was
        never persisted; those releases fall back to the stable default
        parameters (see :meth:`PolicyRegistry.get_or_default`) so they keep
        evaluating/promoting deterministically instead of failing on open.
        """
        raw = self.store.get_policy(release.policy_version)
        if raw is None:
            logger.info(
                "policy fallback release=%s version=%s -> stable defaults",
                release.release_id, release.policy_version,
            )
        return self.policies.get_or_default(release.policy_version)

    def _require_release(self, release_id: str) -> Release:
        release = self.store.get_release(release_id)
        if release is None:
            raise ReleaseNotFoundError(
                "release_not_found", details={"release_id": release_id}
            )
        return release

    @staticmethod
    def _require_active(release: Release) -> None:
        if release.status in _TERMINAL:
            raise ReleaseFinishedError(
                "release_finished", details={"status": release.status.value}
            )

    # ---- idempotency ledger -------------------------------------------
    @staticmethod
    def _ledger_key(action: str, payload: dict) -> str:
        return json.dumps({"action": action, "payload": payload}, sort_keys=True)

    def _remember(self, request_id: str, action: str, result) -> None:
        if not request_id:
            return
        payload = self._result_to_dict(result)
        self.store.remember_decision(
            result.release_id, request_id, self._ledger_key(action, payload)
        )

    def _replay_decision(
        self, release_id: str, request_id: str, stored: str, expect_action: str
    ):
        envelope = json.loads(stored)
        if envelope["action"] != expect_action:
            raise IllegalStateError(
                "illegal_state",
                details={
                    "problem": "request_id already used with a different action",
                    "original_action": envelope["action"],
                    "attempted_action": expect_action,
                },
            )
        payload = envelope["payload"]
        logger.info("idempotent replay release=%s action=%s", release_id, expect_action)
        if expect_action == "promote":
            return PromotionResult(
                release_id=payload["release_id"],
                from_status=ReleaseStatus(payload["from_status"]),
                to_status=ReleaseStatus(payload["to_status"]),
                step_index=payload["step_index"],
                new_version_percent=payload["new_version_percent"],
                evaluated=payload["evaluated"],
                rolled_back=payload["rolled_back"],
                reason=payload["reason"],
            )
        return RollbackResult(
            release_id=payload["release_id"],
            from_status=ReleaseStatus(payload["from_status"]),
            to_status=ReleaseStatus(payload["to_status"]),
            trigger=payload["trigger"],
            reason=payload["reason"],
            idempotent_hit=True,
        )

    @staticmethod
    def _result_to_dict(result) -> dict:
        if isinstance(result, PromotionResult):
            return {
                "release_id": result.release_id,
                "from_status": result.from_status.value,
                "to_status": result.to_status.value,
                "step_index": result.step_index,
                "new_version_percent": result.new_version_percent,
                "evaluated": result.evaluated,
                "rolled_back": result.rolled_back,
                "reason": result.reason,
            }
        return {
            "release_id": result.release_id,
            "from_status": result.from_status.value,
            "to_status": result.to_status.value,
            "trigger": result.trigger,
            "reason": result.reason,
        }
