"""并发单测：并发推进/回滚的幂等性与状态一致性。

验证性质（多轮重复以暴露竞争）：
- 相同幂等键的并发请求只生效一次；
- 无键推进风暴严格逐阶推进，不越级；
- 回滚风暴恰好一次回滚，无重复回滚/漏回滚；
- 自动回滚风暴只产生一条 auto_rollback 事件；
- promote 与 rollback/auto_check 竞争时结论唯一确定、无矛盾。

日志会打印输入、成功/拒绝统计与最终状态迁移轨迹。
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest

from canary.clock import FrozenClock
from canary.errors import CanaryError, ErrorCode
from canary.metrics import MetricsRegistry
from canary.models import ReleaseStatus
from canary.service import ReleaseService
from canary.store import Store

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("test.concurrency")
log.setLevel(logging.INFO)

ROUNDS = 20
WORKERS = 16


@dataclass
class Outcome:
    ok: int = 0
    rejected: dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.rejected is None:
            self.rejected = {}


def make_service() -> tuple[ReleaseService, FrozenClock, MetricsRegistry]:
    clock = FrozenClock(1_000.0)
    metrics = MetricsRegistry(clock)
    return ReleaseService(Store(), metrics, clock), clock, metrics


def run_concurrently(fn, count: int) -> list:
    barrier = threading.Barrier(count)

    def task() -> object:
        barrier.wait()  # 最大化竞争：所有线程同时进入
        try:
            return ("ok", fn())
        except CanaryError as exc:
            return ("reject", exc.code.value)

    with ThreadPoolExecutor(max_workers=count) as pool:
        return list(pool.map(lambda _: task(), range(count)))


def summarize(results: list) -> Outcome:
    out = Outcome()
    for kind, value in results:
        if kind == "ok":
            out.ok += 1
        else:
            out.rejected[value] = out.rejected.get(value, 0) + 1
    return out


def event_trace(service: ReleaseService, rid: str) -> list[str]:
    return [
        f"{e.action}:{e.from_status}->{e.to_status}@stage{ e.detail.get('stage_index', '-') }"
        for e in service.get_release(rid).events
    ]


def test_concurrent_register_same_idempotency_key() -> None:
    for rnd in range(ROUNDS):
        service, _, _ = make_service()
        results = run_concurrently(
            lambda: service.register_release(
                release_id="r1", service="s", environment="e",
                old_version="v1", new_version="v2", idempotency_key="k-register",
            ),
            WORKERS,
        )
        out = summarize(results)
        release = service.get_release("r1")
        log.info("第%d轮 登记风暴: 成功返回=%d 拒绝=%s 事件数=%d",
                 rnd, out.ok, out.rejected, len(release.events))
        assert out.ok == WORKERS  # 幂等重试全部成功返回同一发布单
        assert len(release.events) == 1  # 只登记一次


def test_concurrent_promote_same_key_applied_once() -> None:
    for rnd in range(ROUNDS):
        service, _, _ = make_service()
        service.register_release(
            release_id="r1", service="s", environment="e",
            old_version="v1", new_version="v2",
        )
        results = run_concurrently(
            lambda: service.promote("r1", idempotency_key="k-promote"), WORKERS
        )
        out = summarize(results)
        release = service.get_release("r1")
        log.info("第%d轮 同键推进: 成功=%d 拒绝=%s 轨迹=%s",
                 rnd, out.ok, out.rejected, event_trace(service, "r1"))
        assert out.ok == WORKERS
        assert release.status is ReleaseStatus.CANARY
        assert release.stage_index == 0
        assert sum(1 for e in release.events if e.action == "promote") == 1


def test_concurrent_promote_storm_advances_strictly_in_order() -> None:
    # 3 个台阶 + 最后全量，共 4 次合法推进；其余必须拒绝，且不允许越级。
    for rnd in range(ROUNDS):
        service, _, _ = make_service()
        service.register_release(
            release_id="r1", service="s", environment="e",
            old_version="v1", new_version="v2",
        )
        results = run_concurrently(lambda: service.promote("r1"), WORKERS)
        out = summarize(results)
        release = service.get_release("r1")
        promotes = [e for e in release.events if e.action == "promote"]
        order = [e.detail["stage_index"] for e in promotes]
        log.info("第%d轮 无键推进风暴: 成功=%d 拒绝=%s 台阶顺序=%s 终态=%s",
                 rnd, out.ok, out.rejected, order, release.status.value)
        assert out.ok == 4, out.rejected
        assert order == [0, 1, 2, 3]
        assert release.status is ReleaseStatus.PROMOTED
        assert out.rejected.get(ErrorCode.RELEASE_ALREADY_FINISHED.value, 0) == WORKERS - 4


def test_concurrent_rollback_storm_only_one_rollback() -> None:
    for rnd in range(ROUNDS):
        service, _, _ = make_service()
        service.register_release(
            release_id="r1", service="s", environment="e",
            old_version="v1", new_version="v2",
        )
        service.promote("r1")
        results = run_concurrently(lambda: service.rollback("r1", reason="storm"), WORKERS)
        out = summarize(results)
        release = service.get_release("r1")
        rollbacks = [e for e in release.events if e.action in ("rollback", "auto_rollback")]
        log.info("第%d轮 回滚风暴: 成功=%d 拒绝=%s 回滚事件数=%d",
                 rnd, out.ok, out.rejected, len(rollbacks))
        assert out.ok == 1  # 恰好一次回滚生效
        assert out.rejected.get(ErrorCode.RELEASE_ALREADY_FINISHED.value, 0) == WORKERS - 1
        assert len(rollbacks) == 1
        assert release.status is ReleaseStatus.ROLLED_BACK


def test_concurrent_rollback_same_key_all_return() -> None:
    for rnd in range(ROUNDS):
        service, _, _ = make_service()
        service.register_release(
            release_id="r1", service="s", environment="e",
            old_version="v1", new_version="v2",
        )
        service.promote("r1")
        results = run_concurrently(
            lambda: service.rollback("r1", idempotency_key="k-rollback"), WORKERS
        )
        out = summarize(results)
        release = service.get_release("r1")
        rollbacks = [e for e in release.events if e.action in ("rollback", "auto_rollback")]
        log.info("第%d轮 同键回滚: 成功返回=%d 回滚事件数=%d", rnd, out.ok, len(rollbacks))
        assert out.ok == WORKERS
        assert len(rollbacks) == 1
        assert release.status is ReleaseStatus.ROLLED_BACK


def _bad_window(service: ReleaseService, clock: FrozenClock, metrics: MetricsRegistry) -> None:
    """把发布推进到 stage0 并制造“窗口已满且失败率超阈值”的条件。"""
    service.promote("r1")
    for _ in range(20):
        metrics.record("s", "e", False)  # 100% 失败
    clock.advance(60.0)  # 观察窗口满


def test_concurrent_auto_check_storm_single_auto_rollback() -> None:
    for rnd in range(ROUNDS):
        service, clock, metrics = make_service()
        service.register_release(
            release_id="r1", service="s", environment="e",
            old_version="v1", new_version="v2",
        )
        _bad_window(service, clock, metrics)
        results = run_concurrently(lambda: service.auto_check("r1"), WORKERS)
        out = summarize(results)
        release = service.get_release("r1")
        auto = [e for e in release.events if e.action == "auto_rollback"]
        log.info("第%d轮 自动检查风暴: 调用成功=%d 拒绝=%s auto_rollback事件=%d 终态=%s",
                 rnd, out.ok, out.rejected, len(auto), release.status.value)
        assert out.ok == WORKERS  # auto_check 本身不抛异常
        assert len(auto) == 1  # 只自动回滚一次，无重复回滚
        assert release.status is ReleaseStatus.ROLLED_BACK


def test_concurrent_promote_vs_auto_check_has_unique_consistent_outcome() -> None:
    # 阶段 1：真实并发风暴。锁使所有操作线性化，只断言不变量：
    # 最多一次自动回滚、不越级、不出现矛盾终态。
    saw_rollback = False
    saw_canary = False
    for rnd in range(ROUNDS):
        service, clock, metrics = make_service()
        service.register_release(
            release_id="r1", service="s", environment="e",
            old_version="v1", new_version="v2",
        )
        _bad_window(service, clock, metrics)
        barrier = threading.Barrier(WORKERS)

        def task(i: int):
            barrier.wait()
            try:
                if i % 2 == 0:
                    return ("ok", "promote", service.promote("r1").status.value)
                return ("ok", "auto", service.auto_check("r1")[0].status.value)
            except CanaryError as exc:
                return ("reject", "exc", exc.code.value)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            results = list(pool.map(task, range(WORKERS)))

        release = service.get_release("r1")
        trace = event_trace(service, "r1")
        auto = [e for e in release.events if e.action == "auto_rollback"]
        assert len(auto) <= 1
        promotes = [e for e in release.events if e.action == "promote"]
        assert [e.detail["stage_index"] for e in promotes] in ([], [0], [0, 1])
        assert release.status in (ReleaseStatus.ROLLED_BACK, ReleaseStatus.CANARY)
        # 任何被拒绝的请求都必须带可区分原因（越级/终态），不允许“未知错误”。
        for kind, _name, value in results:
            if kind == "reject":
                assert value in (
                    ErrorCode.RELEASE_ALREADY_FINISHED.value,
                    ErrorCode.STAGE_SKIPPED.value,
                    ErrorCode.INVALID_STAGE.value,
                )
        saw_rollback |= release.status is ReleaseStatus.ROLLED_BACK
        saw_canary |= release.status is ReleaseStatus.CANARY
        log.info("第%d轮 promote vs auto_check 风暴: 终态=%s 轨迹=%s",
                 rnd, release.status.value, trace)

    # 阶段 2：锁的线性化意味着并发结果只能是两种顺序之一。
    # 分别确定性地验证两种交错，结论必须各自自洽且不矛盾。

    # 交错 A：自动回滚先线性化 -> 随后的推进被拒绝（终态重复操作）。
    service, clock, metrics = make_service()
    service.register_release(
        release_id="r1", service="s", environment="e",
        old_version="v1", new_version="v2",
    )
    _bad_window(service, clock, metrics)
    release, verdict = service.auto_check("r1")
    assert verdict.should_rollback and release.status is ReleaseStatus.ROLLED_BACK
    with pytest.raises(CanaryError) as exc_info:
        service.promote("r1")
    assert exc_info.value.code is ErrorCode.RELEASE_ALREADY_FINISHED
    log.info("交错A(自动回滚先): 后续promote被拒绝 code=%s", exc_info.value.code.value)

    # 交错 B：推进先线性化 -> 新台阶观测窗口重置，auto_check 不会误回滚。
    service, clock, metrics = make_service()
    service.register_release(
        release_id="r1", service="s", environment="e",
        old_version="v1", new_version="v2",
    )
    _bad_window(service, clock, metrics)
    release = service.promote("r1")  # 抢到锁：stage0 -> stage1，窗口重新计时
    assert release.stage_index == 1 and release.status is ReleaseStatus.CANARY
    release, verdict = service.auto_check("r1")
    assert not verdict.should_rollback and not verdict.observation_complete
    assert release.status is ReleaseStatus.CANARY
    log.info("交错B(推进先): 新台阶 elapsed=%g 判定=%s，未回滚",
             verdict.elapsed_seconds, verdict.detail.get("decision"))
    # 新台阶窗口内必须重新累积信号（旧台阶样本不会泄漏进新窗口）。
    for _ in range(10):
        metrics.record("s", "e", False)
    clock.advance(120.0)
    release, verdict = service.auto_check("r1")
    assert verdict.should_rollback and release.status is ReleaseStatus.ROLLED_BACK
    log.info("交错B后续: 新台阶窗口满 -> auto_rollback 生效，终态=%s", release.status.value)

    # 风暴至少真实产生过其中一种交错。
    assert saw_rollback or saw_canary


def test_concurrent_promote_vs_rollback_keyed() -> None:
    for rnd in range(ROUNDS):
        service, _, _ = make_service()
        service.register_release(
            release_id="r1", service="s", environment="e",
            old_version="v1", new_version="v2",
        )
        barrier = threading.Barrier(2)

        def do_promote():
            barrier.wait()
            try:
                service.promote("r1", idempotency_key="kp")
                return "promoted"
            except CanaryError as exc:
                return exc.code.value

        def do_rollback():
            barrier.wait()
            try:
                service.rollback("r1", idempotency_key="kr")
                return "rolled_back"
            except CanaryError as exc:
                return exc.code.value

        with ThreadPoolExecutor(max_workers=2) as pool:
            pr, rr = list(pool.map(lambda f: f(), [do_promote, do_rollback]))

        release = service.get_release("r1")
        log.info("第%d轮 promote vs rollback: promote=%s rollback=%s 终态=%s 轨迹=%s",
                 rnd, pr, rr, release.status.value, event_trace(service, "r1"))
        assert release.status in (ReleaseStatus.ROLLED_BACK, ReleaseStatus.CANARY)
        rollbacks = [e for e in release.events if e.action in ("rollback", "auto_rollback")]
        assert len(rollbacks) <= 1
        if release.status is ReleaseStatus.ROLLED_BACK:
            assert len(rollbacks) == 1
