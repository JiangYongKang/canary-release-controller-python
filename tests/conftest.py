"""Shared pytest fixtures.

Unit-test logs print *input*, *state transitions* and *judgement basis*:
the engine logger emits ``register/observe/evaluate input=...``,
``transition ...`` and ``judgement=...`` records at INFO level and we attach
a handler that also echoes them to stdout (visible with ``pytest -s``).
"""

from __future__ import annotations

import logging
import sys

import pytest

from canary.models import ObservationPoint
from canary.service import CanaryService

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
engine_logger = logging.getLogger("canary.engine")
engine_logger.setLevel(logging.INFO)
engine_logger.addHandler(handler)
engine_logger.propagate = False


class FakeClock:
    def __init__(self) -> None:
        self.t = 0

    def __call__(self) -> str:
        self.t += 1
        return f"t{self.t}"


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def service(clock):
    return CanaryService(clock=clock)


@pytest.fixture
def fill_window():
    def _fill(service, release_id, step_index, *, minutes, total, failed):
        """Feed a complete, deterministic observation window."""
        for minute in range(minutes):
            service.observe(
                release_id,
                step_index,
                ObservationPoint(
                    minute_offset=minute,
                    total_requests=total,
                    failed_requests=failed,
                ),
            )

    return _fill
