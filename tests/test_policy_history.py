"""策略版本化与历史复现单测。

覆盖：
- 策略新版本登记、内容去重、按版本读取；
- 缺失字段旧记录的稳定缺省迁移；
- 策略变更后已结束发布的结论不被改写；
- 相同输入+相同策略重复评估结论一致。

日志打印策略输入、版本迁移与复现判定依据。
"""

from __future__ import annotations

import logging

import pytest

from canary.clock import FrozenClock
from canary.errors import CanaryError, ErrorCode
from canary.metrics import MetricsRegistry
from canary.models import ReleaseStatus
from canary.policy import DEFAULT_POLICY, migrate_policy, policy_fingerprint
from canary.service import ReleaseService
from canary.store import Store

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("test.policy")
log.setLevel(logging.INFO)


def make_service() -> tuple[ReleaseService, FrozenClock, MetricsRegistry]:
    clock = FrozenClock(1_000.0)
    metrics = MetricsRegistry(clock)
    return ReleaseService(Store(), metrics, clock), clock, metrics


def make_unhealthy(service, clock, metrics, rid="r1", svc="s", env="e", fails=20) -> None:
    service.promote(rid)
    for _ in range(fails):
        metrics.record(svc, env, False)
    clock.advance(60.0)  # 默认策略 stage0 observe=60


def test_default_policy_created_on_init() -> None:
    service, _, _ = make_service()
    v1 = service.get_policy_version("default", 1)
    assert v1 == DEFAULT_POLICY
    log.info("初始化缺省策略 v1: threshold=%s stages=%s",
             v1["failure_rate_threshold"], [(s["weight"], s["observe_seconds"]) for s in v1["stages"]])


def test_register_version_and_dedup() -> None:
    service, _, _ = make_service()
    tighter = {**DEFAULT_POLICY, "failure_rate_threshold": 0.01}
    v2 = service.register_policy_version("default", tighter)
    assert v2["version"] == 2
    log.info("登记更严阈值策略 => v%s fingerprint=%s", v2["version"], v2["fingerprint"])
    # 相同内容重复提交，不产生新版本
    v2_again = service.register_policy_version("default", {**DEFAULT_POLICY, "failure_rate_threshold": 0.01})
    assert v2_again["version"] == 2
    # 再变更台阶 -> v3
    looser_steps = {
        **DEFAULT_POLICY,
        "stages": [
            {"weight": 25, "observe_seconds": 30.0},
            {"weight": 100, "observe_seconds": 60.0},
        ],
    }
    v3 = service.register_policy_version("default", looser_steps)
    assert v3["version"] == 3
    assert service.get_policy_version("default", 2)["failure_rate_threshold"] == 0.01
    assert service.get_policy_version("default", 3)["stages"][0]["weight"] == 25
    log.info("台阶变更 => v%s；按版本读取互不影响", v3["version"])


def test_register_version_unknown_policy() -> None:
    service, _, _ = make_service()
    with pytest.raises(CanaryError) as info:
        service.register_policy_version("nope", DEFAULT_POLICY)
    assert info.value.code is ErrorCode.POLICY_NOT_FOUND


def test_legacy_missing_fields_use_stable_defaults() -> None:
    # 模拟非常老的记录：只有 stages，且台阶缺少 observe_seconds
    legacy = {"stages": [{"weight": 5}, {"weight": 100}]}
    migrated = migrate_policy(legacy)
    log.info("旧记录 %s 迁移 => threshold=%s min_samples=%s min_observe=%s",
             legacy, migrated["failure_rate_threshold"], migrated["min_samples"],
             migrated["min_observe_seconds"])
    assert migrated["failure_rate_threshold"] == DEFAULT_POLICY["failure_rate_threshold"]
    assert migrated["min_samples"] == DEFAULT_POLICY["min_samples"]
    # 缺失的台阶观察时长回退到 min_observe_seconds 缺省
    assert migrated["stages"][0]["observe_seconds"] == DEFAULT_POLICY["min_observe_seconds"]
    # 迁移确定性
    assert migrate_policy(legacy) == migrated
    assert policy_fingerprint(migrate_policy(legacy)) == policy_fingerprint(migrate_policy(legacy))


@pytest.mark.parametrize("bad", [
    {"stages": [{"weight": 200, "observe_seconds": 10}]},
    {"stages": [{"weight": 50, "observe_seconds": -1}]},
    {"stages": [{"weight": 100}, {"weight": 10}]},   # 权重非递增
    {"stages": []},                                    # 空台阶
    {"failure_rate_threshold": 2.0},                  # 阈值越界
    {"min_samples": 0},                                # 样本下限非法
])
def test_migration_rejects_bad_records(bad: dict) -> None:
    with pytest.raises(CanaryError) as info:
        migrate_policy(bad)
    assert info.value.code is ErrorCode.POLICY_MIGRATION_ERROR
    log.info("非法旧记录 %s 被拒绝: %s", bad, info.value.reason)


def test_policy_change_does_not_rewrite_finished_history() -> None:
    service, clock, metrics = make_service()

    # 1) 用 v1(threshold=0.05) 发布并自动回滚，结束发布
    service.register_release(
        release_id="r-old", service="s", environment="e",
        old_version="v1", new_version="v2", policy_version=1,
    )
    make_unhealthy(service, clock, metrics, rid="r-old")
    release, verdict = service.auto_check("r-old")
    assert verdict.should_rollback and release.status is ReleaseStatus.ROLLED_BACK
    old_snapshot = {
        "threshold": release.policy["failure_rate_threshold"],
        "fingerprint": policy_fingerprint(release.policy),
        "version": release.policy_version,
    }
    old_events = len(release.events)
    log.info("旧发布在 v1(threshold=0.05) 下结论: ROLLED_BACK, 事件=%d, %s",
             old_events, old_snapshot)

    # 2) 策略变更：阈值收紧到 0.50（20/20=1.0 仍超），再加宽台阶与观察时长
    service.register_policy_version("default", {**DEFAULT_POLICY, "failure_rate_threshold": 0.5})
    service.register_policy_version("default", {
        **DEFAULT_POLICY,
        "failure_rate_threshold": 0.5,
        "stages": [{"weight": 100, "observe_seconds": 300.0}],
    })
    current_v = service.get_policy_version("default", 3)
    log.info("策略已变更到 v3: %s", current_v)

    # 3) 用固化快照复现旧发布历史：结论必须保持一致
    replay = service.reevaluate_history("r-old")
    log.info("策略变更后复现旧发布: reproduced=%s final_status=%s 固化阈值=%s",
             replay["reproduced"], replay["final_status"], replay["snapshot_threshold"])
    assert replay["reproduced"] is True
    assert replay["final_status"] == ReleaseStatus.ROLLED_BACK.value
    assert replay["snapshot_threshold"] == 0.05  # 快照而非当前 0.5
    # 发布单本身未被复现动作改动
    again = service.get_release("r-old")
    assert again.status is ReleaseStatus.ROLLED_BACK
    assert len(again.events) == old_events

    # 4) 新发布默认采用最新策略 v3
    service.register_release(
        release_id="r-new", service="s", environment="e2",
        old_version="v9", new_version="v10",
    )
    new_release = service.get_release("r-new")
    assert new_release.policy_version == 3
    assert len(new_release.stages) == 1
    assert new_release.policy["failure_rate_threshold"] == 0.5
    log.info("新发布采用 v3: stages=%s threshold=%s",
             [(s.weight, s.observe_seconds) for s in new_release.stages],
             new_release.policy["failure_rate_threshold"])


def test_same_input_same_policy_same_conclusion_across_calls() -> None:
    service, clock, metrics = make_service()
    service.register_release(
        release_id="r1", service="s", environment="e",
        old_version="v1", new_version="v2",
    )
    make_unhealthy(service, clock, metrics)
    _, v1 = service.evaluate_health("r1")
    _, v2 = service.evaluate_health("r1")
    # 同一窗口、同一策略，两次评估结论一致
    assert v1.should_rollback == v2.should_rollback
    assert v1.rollback_reason == v2.rollback_reason
    log.info("相同输入重复评估: rollback=%s reason=%s", v1.should_rollback, v1.rollback_reason)
