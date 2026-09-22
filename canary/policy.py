"""发布策略的版本化管理、缺省迁移与纯函数评估。

关键不变量：
- 已结束发布的策略以快照形式保存在发布单中，后续策略版本变更不会改写历史结论；
- 旧记录缺失字段时按 :data:`DEFAULT_POLICY` 的稳定缺省语义补齐；
- 相同 (指标输入, 策略快照) 重复评估必定得到相同结论。
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from canary.errors import CanaryError, ErrorCode
from canary.models import CanaryStage

#: 策略第一个版本（v1）的稳定缺省值。
#: 缺失字段的旧记录永远按 v1 缺省语义迁移。
DEFAULT_POLICY: dict[str, Any] = {
    "failure_rate_threshold": 0.05,
    "stages": [
        {"weight": 10, "observe_seconds": 60.0},
        {"weight": 50, "observe_seconds": 120.0},
        {"weight": 100, "observe_seconds": 180.0},
    ],
    "min_samples": 10,
    "min_observe_seconds": 60.0,
}


def _coerce_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CanaryError(
            ErrorCode.POLICY_MIGRATION_ERROR,
            f"策略字段 {field_name} 必须是数字, 实际: {value!r}",
        )
    return float(value)


def migrate_policy(raw: dict[str, Any]) -> dict[str, Any]:
    """把任意历史版本的策略记录规范化为完整策略。

    缺失字段按 v1 缺省语义补齐；无法识别的结构抛出
    :class:`~canary.errors.CanaryError`（POLICY_MIGRATION_ERROR）。

    迁移是确定性的：同一输入永远得到同一输出，且不依赖当前策略，
    因此策略后续版本的变更不会影响旧快照的迁移结果。
    """
    if not isinstance(raw, dict):
        raise CanaryError(
            ErrorCode.POLICY_MIGRATION_ERROR,
            f"策略记录必须是对象, 实际: {type(raw).__name__}",
        )

    merged = copy.deepcopy(DEFAULT_POLICY)
    # 仅接纳已知字段；未知字段不参与评估，保持缺省语义稳定。
    for key in ("failure_rate_threshold", "min_samples", "min_observe_seconds"):
        if key in raw and raw[key] is not None:
            merged[key] = raw[key]
    if "stages" in raw and raw["stages"] is not None:
        merged["stages"] = raw["stages"]

    threshold = _coerce_number(merged["failure_rate_threshold"], "failure_rate_threshold")
    if not 0.0 <= threshold <= 1.0:
        raise CanaryError(
            ErrorCode.POLICY_MIGRATION_ERROR,
            f"failure_rate_threshold 必须在 [0, 1], 实际: {threshold}",
        )
    merged["failure_rate_threshold"] = threshold

    min_samples = merged["min_samples"]
    if isinstance(min_samples, bool) or not isinstance(min_samples, int) or min_samples < 1:
        raise CanaryError(
            ErrorCode.POLICY_MIGRATION_ERROR,
            f"min_samples 必须为 >=1 的整数, 实际: {min_samples!r}",
        )

    min_observe = _coerce_number(merged["min_observe_seconds"], "min_observe_seconds")
    if min_observe < 0:
        raise CanaryError(
            ErrorCode.POLICY_MIGRATION_ERROR,
            f"min_observe_seconds 不能为负, 实际: {min_observe}",
        )
    merged["min_observe_seconds"] = min_observe

    raw_stages = merged["stages"]
    if not isinstance(raw_stages, list) or not raw_stages:
        raise CanaryError(
            ErrorCode.POLICY_MIGRATION_ERROR,
            "stages 必须是非空列表",
        )

    stages: list[dict[str, Any]] = []
    prev_weight = 0
    for idx, item in enumerate(raw_stages):
        if not isinstance(item, dict):
            raise CanaryError(
                ErrorCode.POLICY_MIGRATION_ERROR,
                f"stages[{idx}] 必须是对象, 实际: {type(item).__name__}",
            )
        # 旧记录台阶缺失 observe_seconds 时，回退到顶层 min_observe_seconds。
        observe = item.get("observe_seconds", min_observe)
        stage = CanaryStage(
            weight=int(item["weight"]),
            observe_seconds=_coerce_number(observe, f"stages[{idx}].observe_seconds"),
        )
        try:
            stage.validate()
        except ValueError as exc:
            raise CanaryError(ErrorCode.POLICY_MIGRATION_ERROR, str(exc)) from exc
        if stage.weight <= prev_weight:
            raise CanaryError(
                ErrorCode.POLICY_MIGRATION_ERROR,
                f"stages[{idx}].weight={stage.weight} 必须严格递增(上一阶={prev_weight})",
            )
        prev_weight = stage.weight
        stages.append({"weight": stage.weight, "observe_seconds": stage.observe_seconds})
    if stages[-1]["weight"] != 100:
        raise CanaryError(
            ErrorCode.POLICY_MIGRATION_ERROR,
            f"最后一阶权重必须为 100, 实际: {stages[-1]['weight']}",
        )
    merged["stages"] = stages
    return merged


def policy_fingerprint(policy: dict[str, Any]) -> str:
    """策略内容的稳定指纹（与键序无关），用于判定内容是否变化。"""
    canonical = json.dumps(policy, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
