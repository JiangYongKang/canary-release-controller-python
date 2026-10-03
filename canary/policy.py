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

* missing ``policy_version``        -> 1 (DEFAULT_POLICY_VERSION)
* missing ``failure_rate_threshold`` -> 0.05
* missing ``min_requests``           -> 100
* missing step ``observe_minutes``   -> 5 per step
* steps stored as bare integers      -> ``[10, 30, ...]`` means
  ``(percent=10, observe=5), ...``
* missing ``steps``                  -> ``[10, 30, 50, 100]``
* missing ``name`` / ``created_at``  -> ``"default"`` / ``""``

A legacy release may also *reference* a policy version whose definition was
never stored in the file.  Such a reference never fails: it resolves to a
synthetic policy built solely from the constants above (see
:meth:`PolicyRegistry.resolve_for_release`), so the same stored release keeps
identical threshold / sample / duration semantics across every restart and
repeated evaluation, in every process.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable

from canary.errors import InvalidPolicyError, PolicyNotFoundError
from canary.models import CanaryStep

if TYPE_CHECKING:  # pragma: no cover - import only for type checkers
    from canary.store import LocalStore

# Stable defaults used when legacy records are missing fields.
DEFAULT_POLICY_VERSION = 1
DEFAULT_FAILURE_RATE_THRESHOLD = 0.05
DEFAULT_MIN_REQUESTS = 100
DEFAULT_OBSERVE_MINUTES = 5
DEFAULT_STEPS = (10, 30, 50, 100)
DEFAULT_POLICY_NAME = "default"


def coerce_step(raw) -> CanaryStep:
    """Accept new-style mapping, ``CanaryStep`` or legacy bare-int percent."""
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


def legacy_default_policy(
    version: int = DEFAULT_POLICY_VERSION,
    *,
    steps: Iterable | None = None,
) -> PromotionPolicy:
    """The one fixed policy used whenever a legacy record lacks a definition.

    Threshold, sample requirement, ramp shape and per-step durations come
    exclusively from the module constants, so the result is fully deterministic
    for a given ``(version, steps)`` input — equal input always yields an equal
    policy, regardless of process, restart count or which other policies now
    exist.  ``steps`` are the release's own stored steps; when absent the
    default ramp is used and therefore not validated (the constants are
    well-formed by construction).
    """
    if steps is None:
        resolved_steps: tuple[CanaryStep, ...] = tuple(
            CanaryStep(percent, DEFAULT_OBSERVE_MINUTES) for percent in DEFAULT_STEPS
        )
    else:
        resolved_steps = tuple(coerce_step(s) for s in steps)
    return PromotionPolicy(
        version=int(version),
        name=DEFAULT_POLICY_NAME,
        steps=resolved_steps,
        failure_rate_threshold=DEFAULT_FAILURE_RATE_THRESHOLD,
        min_requests=DEFAULT_MIN_REQUESTS,
        created_at="",
    )


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
        steps = tuple(coerce_step(v) for v in raw.get("steps", DEFAULT_STEPS))
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
    """Versioned, immutable policy set backed by the (cross-process) store.

    Policies live in the same shared state file as releases, so processes that
    attach later see every version created elsewhere, and a release registered
    with a custom policy can keep evaluating against that exact policy after a
    restart.  Each read/create runs inside one store transaction.
    """

    def __init__(
        self,
        store: "LocalStore",
        clock=None,
    ) -> None:
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
        pre-built :class:`CanaryStep` objects.  Version allocation and insert
        happen as one atomic transaction, so concurrent creators in different
        processes never collide on the same version number.
        """
        coerced = [coerce_step(s) for s in steps]
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
        with self._store.lock():
            existing = self._store.all_policies()
            # A legacy file can contain releases bound to policy versions whose
            # definitions were never persisted.  Treat those "ghost" references
            # as occupied too, so a newly created policy never takes over a
            # version an existing release evaluates against.
            occupied = {int(p["version"]) for p in existing}
            occupied.update(self._store.referenced_policy_versions())
            version = (max(occupied) + 1) if occupied else 1
            policy = PromotionPolicy(
                version=version,
                name=name,
                steps=tuple(coerced),
                failure_rate_threshold=float(failure_rate_threshold),
                min_requests=int(min_requests),
                created_at=self._clock() if self._clock else "",
            )
            self._store.put_policy(policy.to_dict())
            return policy

    def get(self, version: int) -> PromotionPolicy:
        raw = self._store.get_policy(version)
        if raw is None:
            raise PolicyNotFoundError("policy_not_found", details={"version": version})
        return PromotionPolicy.from_dict(raw)

    def resolve_for_release(self, release) -> PromotionPolicy:
        """Resolve the policy governing a stored release, legacy-safe.

        New-style releases resolve to the exact stored definition.  A legacy
        release whose referenced version is missing from the file resolves
        instead to :func:`legacy_default_policy` built from the release's own
        steps: threshold 0.05, min 100 samples, and the release's stored
        per-step durations.  The fallback is synthesized on demand and never
        persisted, so re-evaluation is deterministic across restarts and
        processes and does not mutate the state file.
        """
        raw = self._store.get_policy(release.policy_version)
        if raw is not None:
            return PromotionPolicy.from_dict(raw)
        return legacy_default_policy(
            release.policy_version,
            steps=[s for s in release.steps],
        )

    def latest(self) -> PromotionPolicy:
        versions = self._store.all_policies()
        if not versions:
            raise PolicyNotFoundError("policy_not_found", details={"version": "latest"})
        return PromotionPolicy.from_dict(versions[-1])

    def all_versions(self) -> list[PromotionPolicy]:
        return [PromotionPolicy.from_dict(p) for p in self._store.all_policies()]

    def ensure_default(self) -> None:
        """Atomically seed default policy v1 if no policy exists.

        Safe to call from every process on startup: under the cross-process
        lock at most one process performs the insert.
        """
        with self._store.lock():
            if self._store.all_policies():
                return
            policy = PromotionPolicy(
                version=1,
                name=DEFAULT_POLICY_NAME,
                steps=tuple(
                    CanaryStep(percent, DEFAULT_OBSERVE_MINUTES)
                    for percent in DEFAULT_STEPS
                ),
                failure_rate_threshold=DEFAULT_FAILURE_RATE_THRESHOLD,
                min_requests=DEFAULT_MIN_REQUESTS,
                created_at="",
            )
            self._store.put_policy(policy.to_dict())
