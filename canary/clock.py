"""时钟抽象：便于在测试中控制时间，保证窗口判定可复现。"""

from __future__ import annotations

import time
from datetime import datetime, timezone


class Clock:
    """可被替换/冻结的时钟。"""

    def now_ts(self) -> float:
        """当前 POSIX 时间戳（秒）。"""
        return time.time()

    def now_iso(self) -> str:
        """当前 UTC ISO8601 字符串。"""
        return datetime.now(timezone.utc).isoformat()


class FrozenClock(Clock):
    """固定/可手动推进的时钟，仅用于测试与复现。"""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def now_ts(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds
