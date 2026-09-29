"""Versioned promotion policy management and history reproduction.

Policies are *versioned* and immutable once created.  A release captures the
policy *snapshot* it started with (``policy_snapshot``), so later
threshold/step/duration changes never rewrite conclusions of finished
releases.

Stable default semantics for legacy records
-------------------------------------------
Old records may omit fields.  Missing fields are always filled with the
module-level ``DEFAULT_*`` constants (never with "current" values), so the
same stored record re-evaluated at any time yields the same conclusion:

* missing ``failure_rate_threshold`` -> 0.05
* missing ``min_requests``           -> 100
* missing step ``observe_minutes``   -> 5 per step
* steps stored as bare integers      -> ``[10, 30, ...]`` means
  ``(percent=10, observe=5), ...``
* missing ``name`` / ``created_at``  -> ``"default"`` / ``""``
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from canary.errors import InvalidPolicyError, PolicyNotFoundError
from canary.models import CanaryStep

if TYPE_CHECKING:
    from canary.store import LocalStore

# Stable defaults used when legacy records are missing fields.
DEFAULT_FAILURE_RATE_THRESHOLD = 0.05
DEFAULT_MIN_REQUESTS = 100
DEFAULT_OBSERVE_MINUTES = 5
DEFAULT_STEPS = (10, 30, 50, 100)
DEFAULT_POLICY_NAME = "default"


def _coerce_step(raw) -> CanaryStep:
    """Accept new-style mapping or legacy bare-int percent."""
    if isinstance(raw, CanaryStep):
        percent = raw.new_version_percent
        minutes = raw.observe_minutes
    elif isinstance(raw, (int, float)) and not isinstance(raw, bool):
        percent = int(raw)
        minutes = DEFAULT_OBSERVE_MINUTES
    elif isinstance(raw, dict):
        if "new_version_percent" not in raw:
            raise InvalidPolicyError(
                "invalid_policy",
                details={"field": "steps", "problem": "missing new_version_percent"},
            )
        percent = int(raw["new_version_percent"])
        minutes = int(raw.get("observe_minutes", DEFAULT_OBSERVE_MINUTES))
    else:
        raise InvalidPolicyError(
            "invalid_policy",
            details={"field": "steps", "problem": f"unsupported step value: {raw!r}"},
        )
    return CanaryStep(new_version_percent=percent, observe_minutes=minutes)


def validate_steps(steps: list[CanaryStep]) -> None:
    if not steps:
        raise InvalidPolicyError(
            "invalid_policy",
            details={"field": "steps", "problem": "must not be empty"},
        )
    last = 0
    for i, step in enumerate(steps):
        if not (1 <= step.new_version_percent <= 100):
            raise InvalidPolicyError(
                "invalid_policy",
                details={
                    "field": "steps",
                    "index": i,
                    "problem": "percent must be within [1, 100]",
                },
            )
        if step.new_version_percent <= last:
            raise InvalidPolicyError(
                "invalid_policy",
                details={
                    "field": "steps",
                    "index": i,
                    "problem": "percents must be strictly increasing",
                },
            )
        last = step.new_version_percent
        if step.observe_minutes < 1:
            raise InvalidPolicyError(
                "invalid_policy",
                details={
                    "field": "observe_minutes",
                    "index": i,
                    "problem": "must be >= 1",
                },
            )
    if steps[-1].new_version_percent != 100:
        raise InvalidPolicyError(
            "invalid_policy",
            details={"field": "steps", "problem": "last percent must be 100"},
        )


@dataclass(frozen=True)
class PromotionPolicy:
    """Thresholds and ramp shape of a canary strategy."""

    version: int
    name: str
    steps: tuple[CanaryStep, ...]
    failure_rate_threshold: float = DEFAULT_FAILURE_RATE_THRESHOLD
    min_requests: int = DEFAULT_MIN_REQUESTS
    created_at: str = ""

    def to_dict(self) -> dict:
        """Serialize to a stable, JSON-friendly snapshot."""
        return {
            "version": self.version,
            "name": self.name,
            "failure_rate_threshold": self.failure_rate_threshold,
            "min_requests": self.min_requests,
            "created_at": self.created_at,
            "steps": [
                {
                    "new_version_percent": s.new_version_percent,
                    "observe_minutes": s.observe_minutes,
                }
                for s in self.steps
            ],
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "PromotionPolicy":
        """Reconstruct a policy from a stored snapshot/legacy record.

        Missing fields are filled with stable defaults — see module docstring.
        Deterministic: equal ``raw`` input always produces an equal policy.
        """
        if not isinstance(raw, dict):
            raise InvalidPolicyError(
                "invalid_policy", details={"problem": "policy record must be an object"}
            )
        steps = tuple(_coerce_step(v) for v in raw.get("steps", DEFAULT_STEPS))
        validate_steps(list(steps))
        threshold = raw.get("failure_rate_threshold", DEFAULT_FAILURE_RATE_THRESHOLD)
        min_requests = raw.get("min_requests", DEFAULT_MIN_REQUESTS)
        if not (0.0 <= float(threshold) <= 1.0):
            raise InvalidPolicyError(
                "invalid_policy",
                details={"field": "failure_rate_threshold", "problem": "out of [0,1]"},
            )
        if int(min_requests) < 0:
            raise InvalidPolicyError(
                "invalid_policy",
                details={"field": "min_requests", "problem": "must be >= 0"},
            )
        return cls(
            version=int(raw.get("version", 1)),
            name=str(raw.get("name", DEFAULT_POLICY_NAME)),
            steps=steps,
            failure_rate_threshold=float(threshold),
            min_requests=int(min_requests),
            created_at=str(raw.get("created_at", "")),
        )


class PolicyRegistry:
    """Versioned, immutable policy store backed by the shared LocalStore.

    Policy versions live in the same durable state file (and the same
    cross-process transactions) as releases, so a non-default policy created
    by one entry point is visible to every other entry point and survives
    process/machine restarts.  With an in-memory :class:`LocalStore` the
    behaviour is identical to the previous process-local registry.
    """

    def __init__(self, store: LocalStore | None = None, clock=None) -> None:
        if store is None:
            # Local import: canary.store imports PromotionPolicy at module load.
            from canary.store import LocalStore

            store = LocalStore()
        self._store = store
        self._clock = clock

    def create(
        self,
        name: str,
        steps,
        *,
        failure_rate_threshold: float = DEFAULT_FAILURE_RATE_THRESHOLD,
        min_requests: int = DEFAULT_MIN_REQUESTS,
        observe_minutes: int = DEFAULT_OBSERVE_MINUTES,
    ) -> PromotionPolicy:
        """Create a new immutable version.

        ``steps`` may be bare percents (all sharing ``observe_minutes``) or
        pre-built :class:`CanaryStep` objects.
        """
        with self._store.transaction():
            existing = self._store.all_policy_dicts()
            coerced = [_coerce_step(s) for s in steps]
            # Bare-int form uses the call-level observe_minutes for all steps.
            if steps and all(not isinstance(s, CanaryStep) for s in steps):
                coerced = [
                    CanaryStep(s.new_version_percent, observe_minutes) for s in coerced
                ]
            validate_steps(coerced)
            if not (0.0 <= failure_rate_threshold <= 1.0):
                raise InvalidPolicyError(
                    "invalid_policy",
                    details={"field": "failure_rate_threshold", "problem": "out of [0,1]"},
                )
            if min_requests < 0:
                raise InvalidPolicyError(
                    "invalid_policy",
                    details={"field": "min_requests", "problem": "must be >= 0"},
                )
            if observe_minutes < 1:
                raise InvalidPolicyError(
                    "invalid_policy",
                    details={"field": "observe_minutes", "problem": "must be >= 1"},
                )
            version = (max(int(p["version"]) for p in existing) + 1) if existing else 1
            policy = PromotionPolicy(
                version=version,
                name=name,
                steps=tuple(coerced),
                failure_rate_threshold=float(failure_rate_threshold),
                min_requests=int(min_requests),
                created_at=self._clock() if self._clock else "",
            )
            self._store.put_policy(policy)
            return policy

    def get(self, version: int) -> PromotionPolicy:
        raw = self._store.get_policy_dict(version)
        if raw is None:
            raise PolicyNotFoundError(
                "policy_not_found", details={"version": version}
            )
        return PromotionPolicy.from_dict(raw)

    def latest(self) -> PromotionPolicy:
        versions = self._store.all_policy_dicts()
        if not versions:
            raise PolicyNotFoundError(
                "policy_not_found", details={"version": "latest"}
            )
        return PromotionPolicy.from_dict(versions[-1])

    def all_versions(self) -> list[PromotionPolicy]:
        return [PromotionPolicy.from_dict(p) for p in self._store.all_policy_dicts()]
