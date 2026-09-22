"""健康判定单测：观测窗口边界、样本数边界、阈值边界（> 才回滚）。"""

from __future__ import annotations

import logging

import pytest

from canary import health
from canary.models import MetricSample

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("test.health")


def make_samples(failures: int, successes: int) -> list[MetricSample]:
    return [MetricSample(ts=1.0, success=False)] * failures + [
        MetricSample(ts=1.0, success=True)
    ] * successes


def run(failures: int, successes: int, elapsed: float, observe: float = 60.0,
        threshold: float = 0.05, min_samples: int = 10) -> health.HealthVerdict:
    samples = make_samples(failures, successes)
    verdict = health.evaluate(
        samples,
        elapsed_seconds=elapsed,
        observe_seconds=observe,
        failure_rate_threshold=threshold,
        min_samples=min_samples,
        evaluated_at="2026-09-22T00:00:00+00:00",
    )
    log.info(
        "输入 failures=%d successes=%d elapsed=%g/%gs threshold=%g min_samples=%d "
        "=> should_rollback=%s 判定依据=%s",
        failures, successes, elapsed, observe, threshold, min_samples,
        verdict.should_rollback, verdict.detail.get("decision"),
    )
    return verdict


def test_window_incomplete_high_failure_does_not_rollback() -> None:
    v = run(10, 0, elapsed=59.9)
    assert v.observation_complete is False
    assert v.should_rollback is False


def test_window_boundary_exactly_full() -> None:
    # elapsed 恰好等于 observe 视为窗口已满
    v = run(10, 0, elapsed=60.0)
    assert v.observation_complete is True
    assert v.should_rollback is True


def test_not_enough_samples_even_full_window() -> None:
    v = run(9, 0, elapsed=60.0)  # 100% 失败但只有 9 条
    assert v.observation_complete is True
    assert v.sample_count == 9
    assert v.should_rollback is False


def test_min_samples_boundary_exactly_met() -> None:
    v = run(1, 9, elapsed=60.0)  # 恰好 10 条，失败率 10% > 5%
    assert v.should_rollback is True


def test_threshold_boundary_equal_is_healthy() -> None:
    v = run(1, 19, elapsed=60.0, threshold=0.05)  # 1/20 == 0.05
    assert v.failure_rate == pytest.approx(0.05)
    assert v.should_rollback is False


def test_threshold_just_above_rolls_back() -> None:
    v = run(2, 19, elapsed=60.0, threshold=0.05)  # 2/21 ≈ 0.0952
    assert v.should_rollback is True
    assert v.rollback_reason is not None


def test_zero_threshold_boundary() -> None:
    # 阈值=0：任何失败(rate>0)都回滚；零失败健康
    assert run(1, 19, 60.0, threshold=0.0).should_rollback is True
    assert run(0, 10, 60.0, threshold=0.0).should_rollback is False


def test_deterministic_same_input_same_output() -> None:
    a = run(2, 18, 60.0)
    b = run(2, 18, 60.0)
    assert a == b


def test_empty_samples() -> None:
    rate, failures, total = health.window_failure_rate([])
    assert rate is None and failures == 0 and total == 0
