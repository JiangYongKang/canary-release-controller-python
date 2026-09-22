"""健康判定：观测窗口内失败信号 -> 回滚结论（纯函数，可复现）。

判定规则（边界明确）：
1. 观测窗口必须已满：elapsed >= observe_seconds，否则 observation_complete=False，
   即使失败率很高也不回滚（避免窗口未满误判）；
2. 窗口内样本数必须达到 min_samples，否则不回滚；
3. 窗口内失败率 > threshold 才回滚（等于阈值视为健康，边界不误判）。
"""

from __future__ import annotations

from typing import Any

from canary.models import HealthVerdict, MetricSample


def window_failure_rate(samples: list[MetricSample]) -> tuple[float | None, int, int]:
    """统计样本：返回 (失败率, 失败数, 总数)；无样本时失败率为 None。"""
    total = len(samples)
    failures = sum(1 for s in samples if not s.success)
    if total == 0:
        return None, 0, 0
    return failures / total, failures, total


def evaluate(
    samples: list[MetricSample],
    *,
    elapsed_seconds: float,
    observe_seconds: float,
    failure_rate_threshold: float,
    min_samples: int,
    evaluated_at: str,
    extra: dict[str, Any] | None = None,
) -> HealthVerdict:
    """纯函数健康评估。相同输入永远得到相同结论。

    判定顺序（边界明确、可解释）：
    1. ``elapsed_seconds < observe_seconds`` -> 观测窗口未满，不回滚；
    2. 样本总数 ``< min_samples`` -> 样本不足，不回滚；
    3. ``failure_rate > threshold`` 才回滚，``failure_rate == threshold`` 视为健康。
    """
    failure_rate, failures, total = window_failure_rate(samples)
    observation_complete = elapsed_seconds >= observe_seconds

    detail: dict[str, Any] = {
        "rules": [
            f"观测窗口: elapsed={elapsed_seconds:g}s "
            f"observe={observe_seconds:g}s -> "
            f"{'已满' if observation_complete else '未满，不回滚'}",
            f"样本数: total={total} min={min_samples} -> "
            f"{'充足' if total >= min_samples else '不足，不回滚'}",
            f"失败率: {None if failure_rate is None else format(failure_rate, '.4f')} "
            f"阈值={failure_rate_threshold:g} 判定回滚要求 failure_rate > threshold",
        ],
    }
    if extra:
        detail.update(extra)

    should_rollback = False
    rollback_reason: str | None = None

    if not observation_complete:
        detail["decision"] = (
            f"OBSERVATION_PENDING: 还需 {observe_seconds - elapsed_seconds:g}s 观察时间"
        )
    elif total < min_samples:
        detail["decision"] = (
            f"NOT_ENOUGH_SAMPLES: 样本 {total} < 最小要求 {min_samples}，不回滚"
        )
    else:
        assert failure_rate is not None  # total>0 时一定非 None
        if failure_rate > failure_rate_threshold:
            should_rollback = True
            rollback_reason = (
                f"failure_rate={format(failure_rate, '.4f')} > "
                f"threshold={failure_rate_threshold:g} "
                f"(failures={failures}/{total}, elapsed={elapsed_seconds:g}s)"
            )
            detail["decision"] = "ROLLBACK: 失败率超过阈值"
        else:
            detail["decision"] = (
                f"HEALTHY: failure_rate={format(failure_rate, '.4f')} <= "
                f"threshold={failure_rate_threshold:g}（等于阈值视为健康）"
            )

    return HealthVerdict(
        should_rollback=should_rollback,
        rollback_reason=rollback_reason,
        observation_complete=observation_complete,
        elapsed_seconds=float(elapsed_seconds),
        failure_rate=failure_rate,
        threshold=float(failure_rate_threshold),
        sample_count=total,
        failure_count=failures,
        min_samples=int(min_samples),
        evaluated_at=evaluated_at,
        detail=detail,
    )
