"""本地观测指标存储与观测窗口聚合（模拟监控系统）。"""

from __future__ import annotations

from threading import RLock

from canary.clock import Clock
from canary.models import MetricSample


class MetricsRegistry:
    """按 (service, environment) 记录成功/失败信号的内存实现。"""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._lock = RLock()
        self._samples: dict[tuple[str, str], list[MetricSample]] = {}

    def record(
        self,
        service: str,
        environment: str,
        success: bool,
        *,
        ts: float | None = None,
        detail: dict | None = None,
    ) -> MetricSample:
        """记录一条信号样本。"""
        sample = MetricSample(
            ts=self._clock.now_ts() if ts is None else float(ts),
            success=bool(success),
            detail=dict(detail or {}),
        )
        key = (service, environment)
        with self._lock:
            bucket = self._samples.setdefault(key, [])
            bucket.append(sample)
            bucket.sort(key=lambda s: s.ts)
        return sample

    def window(
        self,
        service: str,
        environment: str,
        since: float,
        until: float,
    ) -> list[MetricSample]:
        """返回 [since, until] 观测窗口内的样本（按时间升序）。"""
        with self._lock:
            bucket = self._samples.get((service, environment), [])
            return [s for s in bucket if since <= s.ts <= until]

    def reset(self) -> None:
        """清空全部样本（测试用）。"""
        with self._lock:
            self._samples.clear()
