"""本地终端演示：登记 → 灰度扩量 → 越级被拒 → 观测超阈值 → 自动回滚 → 策略变更 → 历史复现。

运行：uv run python -m canary.demo
"""

from __future__ import annotations

from canary.clock import FrozenClock
from canary.errors import CanaryError
from canary.metrics import MetricsRegistry
from canary.policy import DEFAULT_POLICY
from canary.service import ReleaseService
from canary.store import Store


def line(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> None:
    clock = FrozenClock(1_000.0)
    metrics = MetricsRegistry(clock)
    service = ReleaseService(Store(), metrics, clock)

    line("1) 登记发布 checkout/prod 1.2.0 -> 1.3.0（策略 v1 快照固化）")
    release = service.register_release(
        release_id="rel-001",
        service="checkout",
        environment="prod",
        old_version="1.2.0",
        new_version="1.3.0",
    )
    print(f"状态={release.status.value} 台阶权重={[s.weight for s in release.stages]}")

    line("2) 首次扩量：PENDING -> CANARY 10%，观测窗口开始计时")
    service.promote("rel-001")
    release = service.get_release("rel-001")
    print(f"状态={release.status.value} 当前权重={release.current_weight}% "
          f"观察时长={release.stages[0].observe_seconds:g}s")

    line("3) 越级推进被拒绝（当前 10% 直接请求目标阶 2=100%）")
    try:
        service.promote("rel-001", target_stage=2)
    except CanaryError as exc:
        print(f"被拒绝 [{exc.code.value}]: {exc.reason}")

    line("4) 灌入 20 条失败信号；窗口未满时 auto-check 不得误判")
    for _ in range(20):
        metrics.record("checkout", "prod", False)
    _, verdict = service.auto_check("rel-001")
    print(f"elapsed={verdict.elapsed_seconds:g}s -> {verdict.detail['decision']}")

    line("5) 观察窗口走完（+60s），失败率 100% > 5% -> 自动回滚")
    clock.advance(60.0)
    release, verdict = service.auto_check("rel-001")
    print(f"结论: should_rollback={verdict.should_rollback} 依据={verdict.rollback_reason}")
    print(f"发布终态={release.status.value} 权重={release.current_weight}%")

    line("6) 对已结束发布重复回滚/推进均被拒绝（原因可区分）")
    for action in ("rollback", "promote"):
        try:
            getattr(service, action)("rel-001")
        except CanaryError as exc:
            print(f"{action} 被拒绝 [{exc.code.value}]: {exc.reason}")

    line("7) 策略变更（阈值 5% -> 1%）不影响旧发布的历史结论")
    service.register_policy_version(
        "default", {**DEFAULT_POLICY, "failure_rate_threshold": 0.01}
    )
    replay = service.reevaluate_history("rel-001")
    print(
        f"复现一致={replay['reproduced']} 终态={replay['final_status']} "
        f"固化阈值={replay['snapshot_threshold']}（当前最新策略已为 0.01）"
    )
    for item in replay["evaluations"]:
        print(
            f"  - 历史#{item['history_index']} 当时回滚={item['historical_rollback']} "
            f"重放回滚={item['reproduced_rollback']} 匹配={item['matches']} "
            f"({item['decision']})"
        )

    line("8) 状态迁移轨迹（可解释审计）")
    for event in release.events:
        print(f"#{event.seq} {event.action}: {event.from_status} -> {event.to_status} {event.detail}")


if __name__ == "__main__":
    main()
