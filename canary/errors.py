"""Domain error taxonomy.

Every illegal operation is rejected with a *distinct* error code so callers
can distinguish rejection reasons.
"""


class CanaryError(Exception):
    """Base class for all canary domain errors."""

    code = "canary_error"

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details or {}


class ReleaseNotFoundError(CanaryError):
    code = "release_not_found"


class ReleaseAlreadyExistsError(CanaryError):
    code = "release_already_exists"


class ReleaseFinishedError(CanaryError):
    """Operation targets a release that has already ended."""

    code = "release_finished"


class IllegalStateError(CanaryError):
    """Operation is not allowed in the release's current state."""

    code = "illegal_state"


class StepSkippingError(CanaryError):
    """Promotion jumps over the next configured canary step."""

    code = "step_skipping"


class ObservationPendingError(CanaryError):
    """Observation window has not collected enough samples yet."""

    code = "observation_pending"


class UnhealthyRollbackError(CanaryError):
    """Failure signal met/exceeded threshold -> rollback (auto or explicit)."""

    code = "unhealthy_rollback"


class PolicyNotFoundError(CanaryError):
    code = "policy_not_found"


class InvalidPolicyError(CanaryError):
    code = "invalid_policy"


class InvalidArgumentError(CanaryError):
    """Syntactically/semantically invalid request input."""

    code = "invalid_argument"
