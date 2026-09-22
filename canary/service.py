"""编排服务层：登记发布、推进灰度、自动回滚、策略版本化、历史复现。

所有写操作在同一把存储锁内完成读-判-写，保证并发请求串行化与幂等：
- 相同 idempotency_key 的重复请求返回首次结论，不会重复推进/回滚；
- 已在回滚中的并发回滚请求只会观察到同一次 ROLLED_BACK；
- 推进与自动回滚并发时，由锁决定唯一胜者，结论确定且无中间态泄漏。
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any, Callable

from canary import health, statemachine
from canary.clock import Clock
from canary.errors import CanaryError, ErrorCode
from canary.metrics import MetricsRegistry
from canary.models import (
    CanaryStage,
    HealthVerdict,
    MetricSample,
    Release,
    ReleaseStatus,
    TransitionEvent,
)
from canary.policy import (
    DEFAULT_POLICY,
    migrate_policy,
    policy_fingerprint,
)
from canary.store import Store

DEFAULT_POLICY_NAME = "default"


class ReleaseService:
    """灰度发布编排服务（HTTP/CLI 等各入口共享同一实例与逻辑）。"""

    def __init__(
        self,
        store: Store,
        metrics: MetricsRegistry,
        clock: Clock,
        logger: Any | None = None,
        sink: Callable[[str], None] | None = None,
    ) -> None:
        self._store = store
        self._metrics = metrics
        self._clock = clock
        self._sink = sink
        if logger is None:
            logger = logging.getLogger("canary")
        self._log = logger
        self._ensure_default_policy()

    # ------------------------------------------------------------------ #
    # 内部辅助
    # ------------------------------------------------------------------ #
    def _emit(self, message: str) -> None:
        """输出可解释日志：同时走 logger 与可选 sink（测试可捕获）。"""
        self._log.info(message)
        if self._sink is not None:
            self._sink(message)

    def _ensure_default_policy(self) -> None:
        """保证存在 default 策略的 v1 缺省版本。"""
        with self._store.lock():
            if self._store.get_policy(DEFAULT_POLICY_NAME) is None:
                definition = migrate_policy(DEFAULT_POLICY)
                doc = {
                    "name": DEFAULT_POLICY_NAME,
                    "versions": [
                        {
                            "version": 1,
                            "definition": definition,
                            "fingerprint": policy_fingerprint(definition),
                            "created_at": self._clock.now_iso(),
                            "note": "v1 缺省策略",
                        }
                    ],
                }
                self._store.put_policy(DEFAULT_POLICY_NAME, doc)

    @staticmethod
    def _stages_from_policy(policy: dict[str, Any]) -> tuple[CanaryStage, ...]:
        return tuple(
            CanaryStage(weight=int(s["weight"]), observe_seconds=float(s["observe_seconds"]))
            for s in policy["stages"]
        )

    def _append_event(
        self,
        release: Release,
        action: str,
        from_status: ReleaseStatus,
        to_status: ReleaseStatus,
        detail: dict[str, Any],
    ) -> TransitionEvent:
        """在发布单上追加一条状态迁移事件。"""
        seq = len(release.events) + 1
        event = TransitionEvent(
            seq=seq,
            release_id=release.release_id,
            at=self._clock.now_iso(),
            action=action,
            from_status=from_status.value,
            to_status=to_status.value,
            detail=detail,
        )
        release.events.append(event)
        self._emit(
            f"[迁移] release={release.release_id} service={release.service}/"
            f"{release.environment} #{seq} {action}: "
            f"{from_status.value} -> {to_status.value} | {detail}"
        )
        return event

    # ------------------------------------------------------------------ #
    # 策略版本化
    # ------------------------------------------------------------------ #
    def create_policy(self, name: str, *, if_exists: bool = False) -> dict[str, Any]:
        """创建策略并写入 v1 缺省版本。"""
        with self._store.lock():
            existing = self._store.get_policy(name)
            if existing is not None:
                if not if_exists:
                    raise CanaryError(
                        ErrorCode.POLICY_ALREADY_EXISTS,
                        f"策略 {name!r} 已存在",
                    )
                return existing
            definition = migrate_policy(DEFAULT_POLICY)
            doc = {
                "name": name,
                "versions": [
                    {
                        "version": 1,
                        "definition": definition,
                        "fingerprint": policy_fingerprint(definition),
                        "created_at": self._clock.now_iso(),
                        "note": "v1 缺省策略",
                    }
                ],
            }
            self._store.put_policy(name, doc)
            self._emit(f"[策略] 创建策略 name={name} v1={policy_fingerprint(definition)}")
            return doc

    def register_policy_version(self, name: str, definition: dict[str, Any]) -> dict[str, Any]:
        """登记新策略版本；内容与最新版本相同时返回该版本（不产生重复版本）。"""
        normalized = migrate_policy(definition)
        fingerprint = policy_fingerprint(normalized)
        with self._store.lock():
            doc = self._store.get_policy(name)
            if doc is None:
                raise CanaryError(
                    ErrorCode.POLICY_NOT_FOUND,
                    f"策略 {name!r} 不存在，请先创建",
                )
            latest = doc["versions"][-1]
            if latest["fingerprint"] == fingerprint:
                self._emit(
                    f"[策略] name={name} 内容未变化，复用 v{latest['version']} "
                    f"fp={fingerprint}"
                )
                return latest
            # 内容相同的历史版本同样不重复登记。
            for ver in doc["versions"]:
                if ver["fingerprint"] == fingerprint:
                    self._emit(
                        f"[策略] name={name} 内容与历史 v{ver['version']} 相同，复用该版本"
                    )
                    return ver
            new_version = {
                "version": latest["version"] + 1,
                "definition": normalized,
                "fingerprint": fingerprint,
                "created_at": self._clock.now_iso(),
                "note": f"阈值={normalized['failure_rate_threshold']:g} "
                f"台阶={[s['weight'] for s in normalized['stages']]} "
                f"观察={[s['observe_seconds'] for s in normalized['stages']]}",
            }
            doc["versions"].append(new_version)
            self._store.put_policy(name, doc)
            self._emit(
                f"[策略] name={name} 登记新版本 v{new_version['version']} fp={fingerprint}"
            )
            return new_version

    def get_policy_version(self, name: str, version: int) -> dict[str, Any]:
        """读取指定版本策略的完整快照（缺省迁移后）。"""
        with self._store.lock():
            doc = self._store.get_policy(name)
            if doc is None:
                raise CanaryError(
                    ErrorCode.POLICY_NOT_FOUND, f"策略 {name!r} 不存在"
                )
            for ver in doc["versions"]:
                if ver["version"] == version:
                    return migrate_policy(ver["definition"])
            raise CanaryError(
                ErrorCode.POLICY_NOT_FOUND,
                f"策略 {name!r} 不存在 v{version}，"
                f"可用版本: {[v['version'] for v in doc['versions']]}",
            )

    # ------------------------------------------------------------------ #
    # 发布编排
    # ------------------------------------------------------------------ #
    def register_release(
        self,
        *,
        release_id: str,
        service: str,
        environment: str,
        old_version: str,
        new_version: str,
        policy_name: str = DEFAULT_POLICY_NAME,
        policy_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> Release:
        """登记待发布版本并生成发布单（状态 PENDING，策略快照固化）。"""
        if old_version == new_version:
            raise CanaryError(
                ErrorCode.INVALID_PAYLOAD,
                f"old_version 与 new_version 不能相同: {old_version!r}",
            )
        with self._store.lock():
            existing = self._store.get_release(release_id)
            if existing is not None:
                if idempotency_key and idempotency_key in existing.idempotency_keys:
                    self._emit(
                        f"[幂等] release={release_id} 登记请求命中幂等键 "
                        f"{idempotency_key}，返回已存在发布单"
                    )
                    return existing
                raise CanaryError(
                    ErrorCode.RELEASE_ALREADY_EXISTS,
                    f"发布单 {release_id!r} 已存在",
                )

            active = self._store.find_active(service, environment)
            if active is not None:
                raise CanaryError(
                    ErrorCode.ILLEGAL_STATE,
                    f"服务 {service}/{environment} 已有进行中的发布 "
                    f"{active.release_id}({active.status.value})，需先结束",
                )

            doc = self._store.get_policy(policy_name)
            if doc is None:
                raise CanaryError(
                    ErrorCode.POLICY_NOT_FOUND,
                    f"策略 {policy_name!r} 不存在",
                )
            if policy_version is None:
                ver = doc["versions"][-1]
            else:
                match = next(
                    (v for v in doc["versions"] if v["version"] == policy_version),
                    None,
                )
                if match is None:
                    raise CanaryError(
                        ErrorCode.POLICY_NOT_FOUND,
                        f"策略 {policy_name!r} 不存在 v{policy_version}",
                    )
                ver = match

            policy_snapshot = migrate_policy(ver["definition"])
            stages = self._stages_from_policy(policy_snapshot)
            release = Release(
                release_id=release_id,
                service=service,
                environment=environment,
                old_version=old_version,
                new_version=new_version,
                status=ReleaseStatus.PENDING,
                stage_index=-1,
                stages=stages,
                policy=policy_snapshot,
                policy_version=ver["version"],
                created_at=self._clock.now_iso(),
            )
            if idempotency_key:
                release.idempotency_keys.add(idempotency_key)
            self._append_event(
                release,
                "register",
                ReleaseStatus.PENDING,
                ReleaseStatus.PENDING,
                detail={
                    "input": {
                        "old_version": old_version,
                        "new_version": new_version,
                        "policy": policy_name,
                        "policy_version": ver["version"],
                        "fingerprint": ver["fingerprint"],
                    },
                    "weights": [s.weight for s in stages],
                },
            )
            self._store.put_release(release)
            return release

    def promote(
        self,
        release_id: str,
        *,
        target_stage: int | None = None,
        idempotency_key: str | None = None,
    ) -> Release:
        """推进一个灰度台阶；支持显式目标台阶（越级被拒绝）。"""
        with self._store.lock():
            release = self._require_release_locked(release_id)

            if idempotency_key and idempotency_key in release.idempotency_keys:
                self._emit(
                    f"[幂等] release={release_id} promote 命中幂等键 "
                    f"{idempotency_key}，状态保持 {release.status.value}@"
                    f"{release.stage_index}"
                )
                return release

            ok, code, reason = statemachine.can_promote(release, target_stage)
            if not ok:
                self._emit(f"[拒绝] release={release_id} promote: [{code}] {reason}")
                raise CanaryError(code, reason)  # type: ignore[arg-type]

            from_status = release.status
            next_index = (
                release.stage_index + 1
                if release.status is ReleaseStatus.CANARY
                else 0
            )
            to_status = statemachine.next_status_for_stage(
                next_index, release.stages
            )
            release.stage_index = next_index
            release.status = to_status
            if to_status is ReleaseStatus.CANARY:
                release.stage_entered_ts = self._clock.now_ts()
                release.stage_entered_at = self._clock.now_iso()
            else:
                release.finished_at = self._clock.now_iso()

            if idempotency_key:
                release.idempotency_keys.add(idempotency_key)

            stage = release.stages[next_index] if next_index < len(release.stages) else None
            detail: dict[str, Any] = {
                "input": {"target_stage": target_stage},
                "stage_index": next_index,
                "weight": stage.weight if stage else 100,
                "observe_seconds": stage.observe_seconds if stage else None,
            }
            self._append_event(release, "promote", from_status, to_status, detail)
            self._store.put_release(release)
            return release

    def rollback(
        self,
        release_id: str,
        *,
        reason: str = "manual",
        idempotency_key: str | None = None,
    ) -> Release:
        """回滚发布；终态发布的重复回滚被拒绝，并发回滚保持幂等。

        并发语义：所有请求在同一把存储锁内执行。若发布已被另一个请求
        回滚（ROLLED_BACK），后来的回滚按“重复回滚”拒绝；携带相同
        幂等键的重试则返回首次结果，不产生第二次迁移事件。
        """
        with self._store.lock():
            release = self._require_release_locked(release_id)

            if idempotency_key and idempotency_key in release.idempotency_keys:
                self._emit(
                    f"[幂等] release={release_id} rollback 命中幂等键 "
                    f"{idempotency_key}，状态保持 {release.status.value}"
                )
                return release

            ok, code, why = statemachine.can_rollback(release)
            if not ok:
                self._emit(f"[拒绝] release={release_id} rollback: [{code}] {why}")
                raise CanaryError(code, why)  # type: ignore[arg-type]

            from_status = release.status
            release.status = ReleaseStatus.ROLLED_BACK
            release.finished_at = self._clock.now_iso()
            if idempotency_key:
                release.idempotency_keys.add(idempotency_key)
            self._append_event(
                release,
                "rollback",
                from_status,
                ReleaseStatus.ROLLED_BACK,
                detail={"reason": reason, "weight": 0},
            )
            self._store.put_release(release)
            return release

    def _require_release_locked(self, release_id: str) -> Release:
        """读取发布单，不存在则抛 RELEASE_NOT_FOUND（调用方持锁）。"""
        release = self._store.get_release(release_id)
        if release is None:
            raise CanaryError(
                ErrorCode.RELEASE_NOT_FOUND,
                f"发布单 {release_id!r} 不存在",
            )
        return release

    def evaluate_health(self, release_id: str) -> tuple[Release, HealthVerdict]:
        """基于当前台阶观测窗口做一次健康评估（只读 + 记录结论，不改状态）。"""
        with self._store.lock():
            release = self._require_release_locked(release_id)
            verdict = self._evaluate_locked(release)
            self._record_verdict_locked(release, verdict)
            self._store.put_release(release)
            self._emit(
                f"[评估] release={release_id} status={release.status.value} "
                f"stage={release.stage_index} window_complete="
                f"{verdict.observation_complete} samples="
                f"{verdict.failure_count}/{verdict.sample_count}/"
                f"{verdict.min_samples} failure_rate={verdict.failure_rate} "
                f"threshold={verdict.threshold:g} -> "
                f"{verdict.detail.get('decision')}"
            )
            return release, verdict

    def auto_check(self, release_id: str) -> tuple[Release, HealthVerdict]:
        """评估健康，越过阈值则在同一原子段内触发自动回滚。

        推进与自动回滚并发时，二者竞争同一把存储锁：
        - 若评估在推进前拿到锁，回滚先生效，随后的推进看到 ROLLED_BACK 并被拒绝；
        - 若推进先生效，评估会在新台阶的窗口上进行（窗口通常未满 -> 不回滚）。
        无论哪种交错，都不会出现重复回滚或互相矛盾的结论。
        """
        with self._store.lock():
            release = self._require_release_locked(release_id)
            verdict = self._evaluate_locked(release)
            self._record_verdict_locked(release, verdict)
            self._emit(
                f"[自动检查] release={release_id} stage={release.stage_index} "
                f"-> {verdict.detail.get('decision')}"
            )
            if verdict.should_rollback:
                ok, code, why = statemachine.can_rollback(release)
                if ok:
                    from_status = release.status
                    release.status = ReleaseStatus.ROLLED_BACK
                    release.finished_at = self._clock.now_iso()
                    self._append_event(
                        release,
                        "auto_rollback",
                        from_status,
                        ReleaseStatus.ROLLED_BACK,
                        detail={
                            "reason": "automatic: " + (verdict.rollback_reason or ""),
                            "weight": 0,
                        },
                    )
                else:
                    # 已被并发请求结束（回滚/全量），保持既有终态，结论不矛盾。
                    self._emit(
                        f"[自动检查] release={release_id} 评估要求回滚但当前状态 "
                        f"{release.status.value} 不可迁移: [{code}] {why}"
                    )
            self._store.put_release(release)
            return release, verdict

    def _record_verdict_locked(self, release: Release, verdict: HealthVerdict) -> None:
        """保存评估结论到发布单（调用方持锁）。"""
        record = asdict(verdict)
        release.last_verdict = record
        release.verdict_history.append(record)

    def _evaluate_locked(self, release: Release) -> HealthVerdict:
        """对当前台阶执行纯函数健康评估（调用方持锁）。"""
        threshold = release.policy["failure_rate_threshold"]
        min_samples = release.policy["min_samples"]
        if release.status is not ReleaseStatus.CANARY:
            # PENDING/终态没有进行中的观测窗口：给出确定的“不回滚”结论。
            return health.evaluate(
                [],
                elapsed_seconds=0.0,
                observe_seconds=0.0,
                failure_rate_threshold=threshold,
                min_samples=min_samples,
                evaluated_at=self._clock.now_iso(),
                extra={"note": f"状态 {release.status.value} 无进行中的观测窗口"},
            )

        stage = release.stages[release.stage_index]
        entered_ts = release.stage_entered_ts
        now_ts = self._clock.now_ts()
        elapsed = 0.0 if entered_ts is None else max(0.0, now_ts - entered_ts)
        samples = self._metrics.window(
            release.service,
            release.environment,
            since=entered_ts if entered_ts is not None else now_ts,
            until=now_ts,
        )
        return health.evaluate(
            samples,
            elapsed_seconds=elapsed,
            observe_seconds=stage.observe_seconds,
            failure_rate_threshold=threshold,
            min_samples=min_samples,
            evaluated_at=self._clock.now_iso(),
            extra={
                "release_id": release.release_id,
                "stage_index": release.stage_index,
                "weight": stage.weight,
                "observe_seconds": stage.observe_seconds,
                "window_since": entered_ts,
                "window_until": now_ts,
            },
        )

    def get_release(self, release_id: str) -> Release:
        """读取发布单，不存在则抛 RELEASE_NOT_FOUND。"""
        release = self._store.get_release(release_id)
        if release is None:
            raise CanaryError(
                ErrorCode.RELEASE_NOT_FOUND,
                f"发布单 {release_id!r} 不存在",
            )
        return release

    def reevaluate_history(self, release_id: str) -> dict[str, Any]:
        """用发布单内固化的策略快照重放历史评估，验证历史结论不被改写。

        对发布单上记录的每一次历史评估，用其保存的聚合输入（窗口时长、
        样本总数、失败数）重建样本，再以发布单内的策略快照重新调用纯函数
        :func:`canary.health.evaluate`，比较回滚结论是否一致。
        """
        release = self.get_release(release_id)
        threshold = release.policy["failure_rate_threshold"]
        min_samples = release.policy["min_samples"]
        results: list[dict[str, Any]] = []
        all_match = True
        for idx, record in enumerate(release.verdict_history):
            observe = float(record["detail"].get("observe_seconds", 0.0))
            # 用历史聚合数据重建样本：仅失败数/成功数影响纯函数结论。
            rebuilt = [MetricSample(ts=0.0, success=False)] * record["failure_count"] + [
                MetricSample(ts=0.0, success=True)
            ] * (record["sample_count"] - record["failure_count"])
            replayed = health.evaluate(
                rebuilt,
                elapsed_seconds=record["elapsed_seconds"],
                observe_seconds=observe,
                failure_rate_threshold=threshold,
                min_samples=min_samples,
                evaluated_at=record["evaluated_at"],
            )
            matches = replayed.should_rollback == record["should_rollback"]
            all_match = all_match and matches
            results.append(
                {
                    "history_index": idx,
                    "evaluated_at": record["evaluated_at"],
                    "historical_rollback": record["should_rollback"],
                    "reproduced_rollback": replayed.should_rollback,
                    "matches": matches,
                    "decision": replayed.detail.get("decision"),
                    "inputs": {
                        "elapsed_seconds": record["elapsed_seconds"],
                        "sample_count": record["sample_count"],
                        "failure_count": record["failure_count"],
                        "failure_rate": record["failure_rate"],
                        "snapshot_threshold": threshold,
                        "snapshot_min_samples": min_samples,
                    },
                }
            )
        return {
            "release_id": release_id,
            "final_status": release.status.value,
            "policy_version": release.policy_version,
            "policy_fingerprint": policy_fingerprint(release.policy),
            "snapshot_threshold": threshold,
            "reproduced": all_match,
            "evaluations": results,
        }
