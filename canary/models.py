"""领域模型：发布单、阶段、事件、评估结论。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ReleaseStatus(str, Enum):
    """发布状态机。

    合法迁移（详见 canary.statemachine）::

        PENDING -> CANARY_<weight>... -> PROMOTED
        任意非终态  -> ROLLED_BACK
    """

    PENDING = "pending"
    CANARY = "canary"
    PROMOTED = "promoted"
    ROLLED_BACK = "rolled_back"

    @property
    def terminal(self) -> bool:
        return self in (ReleaseStatus.PROMOTED, ReleaseStatus.ROLLED_BACK)


@dataclass(frozen=True)
class CanaryStage:
    """一个灰度台阶：权重百分比 + 该台阶需要观察的秒数。"""

    weight: int
    observe_seconds: float

    def validate(self) -> None:
        """校验台阶定义，非法时抛出 ValueError。"""
        if not isinstance(self.weight, int) or isinstance(self.weight, bool):
            raise ValueError(f"stage weight 必须为整数, 实际: {self.weight!r}")
        if not 0 < self.weight <= 100:
            raise ValueError(f"stage weight 必须在 (0, 100], 实际: {self.weight}")
        if not isinstance(self.observe_seconds, (int, float)) or isinstance(
            self.observe_seconds, bool
        ):
            raise ValueError(
                f"observe_seconds 必须为数字, 实际: {self.observe_seconds!r}"
            )
        if self.observe_seconds < 0:
            raise ValueError(
                f"observe_seconds 不能为负, 实际: {self.observe_seconds}"
            )


@dataclass(frozen=True)
class HealthVerdict:
    """一次健康评估的结论（纯数据，保证可复现）。"""

    should_rollback: bool
    rollback_reason: str | None
    observation_complete: bool
    elapsed_seconds: float
    failure_rate: float | None
    threshold: float
    sample_count: int
    failure_count: int
    min_samples: int
    evaluated_at: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransitionEvent:
    """状态迁移事件，用于历史与审计。"""

    seq: int
    release_id: str
    at: str
    action: str
    from_status: str
    to_status: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class Release:
    """一张灰度发布单及其运行时状态。"""

    release_id: str
    service: str
    environment: str
    old_version: str
    new_version: str
    status: ReleaseStatus
    stage_index: int
    stages: tuple[CanaryStage, ...]
    policy: dict[str, Any]
    policy_version: int
    idempotency_keys: set[str] = field(default_factory=set)
    stage_entered_ts: float | None = None
    stage_entered_at: str | None = None
    created_at: str | None = None
    finished_at: str | None = None
    events: list[TransitionEvent] = field(default_factory=list)
    last_verdict: dict[str, Any] | None = None
    verdict_history: list[dict[str, Any]] = field(default_factory=list)

    @property
    def current_weight(self) -> int:
        """当前台阶的流量权重；终态回滚为 0，全量完成为 100。"""
        if self.status is ReleaseStatus.ROLLED_BACK:
            return 0
        if self.status is ReleaseStatus.PROMOTED:
            return 100
        if self.status is ReleaseStatus.PENDING:
            return 0
        if self.stage_index < 0:
            return 0
        if 0 <= self.stage_index < len(self.stages):
            return self.stages[self.stage_index].weight
        return 0


def release_to_dict(release: Release) -> dict[str, Any]:
    """把发布单序列化为可 JSON 持久化的字典。"""
    return {
        "release_id": release.release_id,
        "service": release.service,
        "environment": release.environment,
        "old_version": release.old_version,
        "new_version": release.new_version,
        "status": release.status.value,
        "stage_index": release.stage_index,
        "stages": [
            {"weight": s.weight, "observe_seconds": s.observe_seconds}
            for s in release.stages
        ],
        "policy": release.policy,
        "policy_version": release.policy_version,
        "idempotency_keys": sorted(release.idempotency_keys),
        "stage_entered_ts": release.stage_entered_ts,
        "stage_entered_at": release.stage_entered_at,
        "created_at": release.created_at,
        "finished_at": release.finished_at,
        "events": [
            {
                "seq": e.seq,
                "release_id": e.release_id,
                "at": e.at,
                "action": e.action,
                "from_status": e.from_status,
                "to_status": e.to_status,
                "detail": e.detail,
            }
            for e in release.events
        ],
        "last_verdict": release.last_verdict,
    }


def release_from_dict(data: dict[str, Any]) -> Release:
    """从字典恢复发布单。"""
    stages = tuple(
        CanaryStage(weight=int(s["weight"]), observe_seconds=float(s["observe_seconds"]))
        for s in data["stages"]
    )
    return Release(
        release_id=data["release_id"],
        service=data["service"],
        environment=data["environment"],
        old_version=data["old_version"],
        new_version=data["new_version"],
        status=ReleaseStatus(data["status"]),
        stage_index=int(data["stage_index"]),
        stages=stages,
        policy=dict(data["policy"]),
        policy_version=int(data["policy_version"]),
        idempotency_keys=set(data.get("idempotency_keys", [])),
        stage_entered_ts=data.get("stage_entered_ts"),
        stage_entered_at=data.get("stage_entered_at"),
        created_at=data.get("created_at"),
        finished_at=data.get("finished_at"),
        events=[
            TransitionEvent(
                seq=int(e["seq"]),
                release_id=e["release_id"],
                at=e["at"],
                action=e["action"],
                from_status=e["from_status"],
                to_status=e["to_status"],
                detail=dict(e.get("detail", {})),
            )
            for e in data.get("events", [])
        ],
        last_verdict=data.get("last_verdict"),
        verdict_history=list(data.get("verdict_history", [])),
    )


@dataclass
class MetricSample:
    """观测窗口内的一条失败/成功信号样本。"""

    ts: float
    success: bool
    detail: dict[str, Any] = field(default_factory=dict)
