"""Domain data models (pure data, no behaviour)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ReleaseStatus(str, Enum):
    """Release state machine states.

    Legal transitions:
        PENDING -> CANARY -> COMPLETED
        PENDING/CANARY -> ROLLED_BACK (via rollback)
    COMPLETED and ROLLED_BACK are terminal.
    """

    PENDING = "pending"
    CANARY = "canary"
    COMPLETED = "completed"
    ROLLED_BACK = "rolled_back"


@dataclass
class CanaryStep:
    """A single traffic ramp step: ``new_version_percent`` of traffic to new."""

    new_version_percent: int
    observe_minutes: int


@dataclass
class ObservationPoint:
    """One observed sample inside an observation window."""

    minute_offset: int
    total_requests: int
    failed_requests: int

    @property
    def fail_rate(self) -> float:
        if self.total_requests <= 0:
            return 0.0
        return self.failed_requests / self.total_requests


@dataclass
class Release:
    """A registered canary release for one (service, environment)."""

    release_id: str
    service: str
    environment: str
    old_version: str
    new_version: str
    status: ReleaseStatus
    policy_version: int
    steps: list[CanaryStep]
    current_step_index: int
    created_at: str
    updated_at: str
    observations: dict[int, list[ObservationPoint]] = field(default_factory=dict)
    finish_reason: str = ""


@dataclass
class PromotionResult:
    release_id: str
    from_status: ReleaseStatus
    to_status: ReleaseStatus
    step_index: int
    new_version_percent: int
    evaluated: bool
    rolled_back: bool
    reason: str


@dataclass
class RollbackResult:
    release_id: str
    from_status: ReleaseStatus
    to_status: ReleaseStatus
    trigger: str
    reason: str
    idempotent_hit: bool


@dataclass
class HistoryEntry:
    """Immutable historical conclusion record."""

    release_id: str
    service: str
    environment: str
    old_version: str
    new_version: str
    final_status: ReleaseStatus
    reason: str
    policy_snapshot: dict
    finished_at: str
