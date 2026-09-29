"""Application facade: single entry point shared by HTTP API, CLI and tests.

On construction it seeds policy v1 with stable defaults so registration works
out of the box; further :meth:`create_policy` calls produce newer immutable
versions.
"""

from __future__ import annotations

from typing import Callable

from canary.engine import CanaryEngine
from canary.models import HistoryEntry, ObservationPoint, PromotionResult, Release, RollbackResult
from canary.policy import (
    DEFAULT_FAILURE_RATE_THRESHOLD,
    DEFAULT_MIN_REQUESTS,
    DEFAULT_OBSERVE_MINUTES,
    PolicyRegistry,
    PromotionPolicy,
)
from canary.store import LocalStore


class CanaryService:
    def __init__(
        self,
        *,
        persist_path: str | None = None,
        clock: Callable[[], str] | None = None,
    ) -> None:
        self.store = LocalStore(persist_path=persist_path)
        self.policies = PolicyRegistry(self.store, clock=clock)
        kwargs = {}
        if clock is not None:
            kwargs["clock"] = clock
        self.engine = CanaryEngine(self.store, self.policies, **kwargs)
        # Safe to run on every startup / in every process: the store-level
        # transaction makes the "insert v1 if missing" check atomic across
        # processes, and it also restores v1 when opening a legacy state file
        # written before policies were persisted.
        self.policies.ensure_default()

    def create_policy(
        self,
        name: str,
        steps: list[int],
        *,
        failure_rate_threshold: float = DEFAULT_FAILURE_RATE_THRESHOLD,
        min_requests: int = DEFAULT_MIN_REQUESTS,
        observe_minutes: int = DEFAULT_OBSERVE_MINUTES,
    ) -> PromotionPolicy:
        return self.policies.create(
            name,
            steps,
            failure_rate_threshold=failure_rate_threshold,
            min_requests=min_requests,
            observe_minutes=observe_minutes,
        )

    def register(self, **kwargs) -> Release:
        return self.engine.register_release(**kwargs)

    def observe(self, release_id: str, step_index: int, point: ObservationPoint) -> None:
        self.engine.record_observation(release_id, step_index, point)

    def promote(self, release_id: str, **kwargs) -> PromotionResult:
        return self.engine.promote(release_id, **kwargs)

    def rollback(self, release_id: str, **kwargs) -> RollbackResult:
        return self.engine.rollback(release_id, **kwargs)

    def evaluate(self, release_id: str) -> tuple[bool, str]:
        return self.engine.evaluate(release_id)

    def get_release(self, release_id: str) -> Release:
        return self.engine.get_release(release_id)

    def history(self, service: str | None = None) -> list[HistoryEntry]:
        return self.store.list_history(service)
