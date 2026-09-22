"""灰度状态机：纯函数式的合法性判定与迁移规则。

规则：
- PENDING 只能进入第 0 个灰度台阶；
- CANARY 只能从当前台阶推进到紧邻的下一阶（禁止越级），
  最后一阶推进后进入 PROMOTED；
- 任意非终态可迁移到 ROLLED_BACK；
- PROMOTED / ROLLED_BACK 为终态，拒绝任何迁移。
"""

from __future__ import annotations

from canary.errors import ErrorCode
from canary.models import CanaryStage, Release, ReleaseStatus


def is_terminal(status: ReleaseStatus) -> bool:
    """状态是否为终态。"""
    return status.terminal


def can_promote(release: Release, target_stage: int | None = None) -> tuple[bool, ErrorCode | None, str]:
    """判定推进是否合法。

    返回 (是否合法, 错误码, 说明)。``target_stage`` 给定时同时校验越级。

    拒绝原因区分：
    - ``RELEASE_ALREADY_FINISHED``：终态发布（promoted/rolled_back）上的重复操作；
    - ``ILLEGAL_STATE``：PENDING 之外的非 canary 状态（防御性）；
    - ``INVALID_STAGE``：目标台阶越界；
    - ``STAGE_SKIPPED``：越级推进（目标不是紧邻的下一阶）。
    """
    if is_terminal(release.status):
        return (
            False,
            ErrorCode.RELEASE_ALREADY_FINISHED,
            f"发布 {release.release_id} 已结束({release.status.value})，不能继续推进",
        )

    if release.status is ReleaseStatus.CANARY:
        next_stage = release.stage_index + 1
    elif release.status is ReleaseStatus.PENDING:
        next_stage = 0
    else:  # pragma: no cover - 当前枚举下不可达
        return (
            False,
            ErrorCode.ILLEGAL_STATE,
            f"状态 {release.status.value} 不允许推进",
        )

    # next_stage == len(stages) 表示从最后一阶全量完成(PROMOTED)，合法；
    # 更大的序号只可能出现在状态损坏时。
    if next_stage > len(release.stages):
        return (
            False,
            ErrorCode.ILLEGAL_STATE,
            f"台阶序号 {release.stage_index} 超出阶段数 {len(release.stages)}",
        )

    if target_stage is not None:
        if next_stage == len(release.stages):
            # 最后一阶之后只能“确认全量”，不存在更靠后的灰度台阶。
            return (
                False,
                ErrorCode.INVALID_STAGE,
                "已处于最后灰度台阶，请直接确认全量(PROMOTE)，不要指定目标台阶",
            )
        if target_stage < 0 or target_stage >= len(release.stages):
            return (
                False,
                ErrorCode.INVALID_STAGE,
                f"目标台阶 {target_stage} 越界，合法范围 [0, {len(release.stages) - 1}]",
            )
        if target_stage != next_stage:
            return (
                False,
                ErrorCode.STAGE_SKIPPED,
                f"禁止越级推进：当前第 {release.stage_index} 阶，"
                f"下一阶必须是 {next_stage}，却请求 {target_stage}",
            )

    if next_stage == len(release.stages):
        return True, None, "允许从最后一阶全量完成(PROMOTED)"
    return True, None, f"允许从第 {release.stage_index} 阶推进到第 {next_stage} 阶"


def can_rollback(release: Release) -> tuple[bool, ErrorCode | None, str]:
    """判定回滚是否合法（终态发布拒绝回滚）。"""
    if release.status is ReleaseStatus.ROLLED_BACK:
        return (
            False,
            ErrorCode.RELEASE_ALREADY_FINISHED,
            f"发布 {release.release_id} 已经回滚，不能重复回滚",
        )
    if release.status is ReleaseStatus.PROMOTED:
        return (
            False,
            ErrorCode.RELEASE_ALREADY_FINISHED,
            f"发布 {release.release_id} 已全量完成，不能再回滚",
        )
    return True, None, f"允许从状态 {release.status.value} 回滚"


def next_status_for_stage(stage_index: int, stages: tuple[CanaryStage, ...]) -> ReleaseStatus:
    """根据台阶序号推导迁移后的状态（最后一阶之后为 PROMOTED）。"""
    if stage_index >= len(stages):
        return ReleaseStatus.PROMOTED
    return ReleaseStatus.CANARY
