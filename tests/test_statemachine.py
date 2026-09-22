"""状态机单测：越级迁移、非法状态、终态重复操作，拒绝原因必须可区分。

每条用例都会通过 logging 打印：输入、状态迁移检查结果与判定依据。
"""

from __future__ import annotations

import logging

import pytest

from canary import statemachine as sm
from canary.errors import ErrorCode
from canary.models import CanaryStage, Release, ReleaseStatus

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("test.statemachine")

STAGES = tuple(CanaryStage(w, 60.0) for w in (10, 50, 100))


def make_release(status: ReleaseStatus, stage_index: int) -> Release:
    return Release(
        release_id="r-sm",
        service="svc",
        environment="prod",
        old_version="v1",
        new_version="v2",
        status=status,
        stage_index=stage_index,
        stages=STAGES,
        policy={"failure_rate_threshold": 0.05, "min_samples": 10},
        policy_version=1,
    )


@pytest.mark.parametrize(
    ("status", "stage", "expected"),
    [
        (ReleaseStatus.PENDING, -1, True),
        (ReleaseStatus.CANARY, 0, True),
        (ReleaseStatus.CANARY, 1, True),
        (ReleaseStatus.CANARY, 2, True),  # 最后一阶 -> PROMOTED 合法
        (ReleaseStatus.PROMOTED, 3, False),
        (ReleaseStatus.ROLLED_BACK, 1, False),
    ],
)
def test_promote_legality_by_state(status: ReleaseStatus, stage: int, expected: bool) -> None:
    release = make_release(status, stage)
    ok, code, reason = sm.can_promote(release)
    log.info("输入 status=%s stage=%s => 判定 ok=%s code=%s 依据=%s", status.value, stage, ok, code, reason)
    assert ok is expected
    if not expected:
        assert code is ErrorCode.RELEASE_ALREADY_FINISHED


def test_skip_from_pending_is_rejected() -> None:
    release = make_release(ReleaseStatus.PENDING, -1)
    log.info("输入: PENDING 直接请求 target_stage=2（越级）")
    ok, code, reason = sm.can_promote(release, target_stage=2)
    log.info("判定: ok=%s code=%s 依据=%s", ok, code, reason)
    assert ok is False
    assert code is ErrorCode.STAGE_SKIPPED


def test_skip_between_canary_stages_is_rejected() -> None:
    release = make_release(ReleaseStatus.CANARY, 0)
    log.info("输入: CANARY stage=0 请求 target_stage=2（跳过 stage=1）")
    ok, code, reason = sm.can_promote(release, target_stage=2)
    log.info("判定: ok=%s code=%s 依据=%s", ok, code, reason)
    assert ok is False
    assert code is ErrorCode.STAGE_SKIPPED


def test_exact_next_stage_is_allowed() -> None:
    release = make_release(ReleaseStatus.CANARY, 0)
    log.info("输入: CANARY stage=0 请求 target_stage=1（紧邻下一阶）")
    ok, code, reason = sm.can_promote(release, target_stage=1)
    log.info("判定: ok=%s code=%s 依据=%s", ok, code, reason)
    assert ok is True
    assert code is None


def test_target_stage_out_of_range() -> None:
    release = make_release(ReleaseStatus.CANARY, 0)
    log.info("输入: CANARY stage=0 请求 target_stage=99（越界）")
    ok, code, reason = sm.can_promote(release, target_stage=99)
    log.info("判定: ok=%s code=%s 依据=%s", ok, code, reason)
    assert ok is False
    assert code is ErrorCode.INVALID_STAGE


def test_rollback_from_terminal_states_is_rejected() -> None:
    for status in (ReleaseStatus.PROMOTED, ReleaseStatus.ROLLED_BACK):
        release = make_release(status, 1)
        ok, code, reason = sm.can_rollback(release)
        log.info("输入 rollback status=%s => 判定 ok=%s code=%s 依据=%s", status.value, ok, code, reason)
        assert ok is False
        assert code is ErrorCode.RELEASE_ALREADY_FINISHED


def test_rollback_from_non_terminal_is_allowed() -> None:
    for status, stage in ((ReleaseStatus.PENDING, -1), (ReleaseStatus.CANARY, 0)):
        release = make_release(status, stage)
        ok, code, reason = sm.can_rollback(release)
        log.info("输入 rollback status=%s => 判定 ok=%s code=%s 依据=%s", status.value, ok, code, reason)
        assert ok is True and code is None


def test_next_status_for_stage() -> None:
    assert sm.next_status_for_stage(0, STAGES) is ReleaseStatus.CANARY
    assert sm.next_status_for_stage(3, STAGES) is ReleaseStatus.PROMOTED
    log.info("next_status_for_stage: stage0=>canary, stage3(=len)=>promoted")
