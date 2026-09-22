"""错误类型与拒绝原因。

所有对非法请求的拒绝都抛出 :class:`CanaryError`，其 :attr:`code` 给出
机器可区分的拒绝原因，:attr:`reason` 给出人类可读说明。
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """可区分的拒绝原因。"""

    RELEASE_NOT_FOUND = "release_not_found"
    RELEASE_ALREADY_EXISTS = "release_already_exists"
    RELEASE_ALREADY_FINISHED = "release_already_finished"
    ILLEGAL_STATE = "illegal_state"
    INVALID_STAGE = "invalid_stage"
    STAGE_SKIPPED = "stage_skipped"
    ALREADY_AT_STAGE = "already_at_stage"
    OBSERVATION_PENDING = "observation_pending"
    HEALTHY = "healthy"
    POLICY_NOT_FOUND = "policy_not_found"
    POLICY_ALREADY_EXISTS = "policy_already_exists"
    POLICY_MIGRATION_ERROR = "policy_migration_error"
    INVALID_PAYLOAD = "invalid_payload"


class CanaryError(Exception):
    """业务错误，携带可区分的错误码。"""

    def __init__(self, code: ErrorCode, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason

    def __str__(self) -> str:  # pragma: no cover - 调试辅助
        return f"[{self.code.value}] {self.reason}"
