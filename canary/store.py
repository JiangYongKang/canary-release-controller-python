"""Local state storage (in-memory + optional JSON persistence).

Stores releases, the idempotency decision ledger and an immutable history
ledger.  A single re-entrant lock serializes all mutations; the engine takes
this same lock around read-modify-write sequences to avoid state races.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict

from canary.models import (
    HistoryEntry,
    ObservationPoint,
    Release,
    ReleaseStatus,
)


class LocalStore:
    """Local state backend simulating artifact / environment / metric state."""

    def __init__(self, *, persist_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self._persist_path = persist_path
        self._releases: dict[str, Release] = {}
        self._active_index: dict[tuple[str, str], str] = {}
        # (release_id, request_id) -> outcome tag ("promoted" / "rolled_back" / ...)
        self._decisions: dict[tuple[str, str], str] = {}
        self._history: list[HistoryEntry] = []
        if persist_path and os.path.exists(persist_path):
            self._load()

    # ---- release CRUD -------------------------------------------------
    def put_release(self, release: Release) -> None:
        with self._lock:
            self._releases[release.release_id] = release
            if release.status not in (
                ReleaseStatus.COMPLETED,
                ReleaseStatus.ROLLED_BACK,
            ):
                self._active_index[(release.service, release.environment)] = (
                    release.release_id
                )
            else:
                self._active_index.pop(
                    (release.service, release.environment), None
                )
            self._flush()

    def get_release(self, release_id: str) -> Release | None:
        with self._lock:
            return self._releases.get(release_id)

    def find_release(self, service: str, environment: str) -> Release | None:
        with self._lock:
            rid = self._active_index.get((service, environment))
            return self._releases.get(rid) if rid else None

    # ---- idempotency ledger -------------------------------------------
    def decision_seen(self, release_id: str, request_id: str) -> str | None:
        """Return remembered outcome for this request, or None."""
        if not request_id:
            return None
        with self._lock:
            return self._decisions.get((release_id, request_id))

    def remember_decision(
        self, release_id: str, request_id: str, outcome: str
    ) -> None:
        if not request_id:
            return
        with self._lock:
            self._decisions[(release_id, request_id)] = outcome
            self._flush()

    # ---- immutable history --------------------------------------------
    def append_history(self, entry: HistoryEntry) -> None:
        with self._lock:
            self._history.append(entry)
            self._flush()

    def list_history(self, service: str | None = None) -> list[HistoryEntry]:
        with self._lock:
            if service is None:
                return list(self._history)
            return [h for h in self._history if h.service == service]

    def lock(self) -> threading.RLock:
        return self._lock

    # ---- (de)serialization --------------------------------------------
    def _flush(self) -> None:
        if not self._persist_path:
            return
        payload = {
            "releases": [_release_to_dict(r) for r in self._releases.values()],
            "decisions": [
                {"release_id": k[0], "request_id": k[1], "outcome": v}
                for k, v in self._decisions.items()
            ],
            "history": [asdict(h) for h in self._history],
        }
        tmp = self._persist_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, self._persist_path)

    def _load(self) -> None:
        with open(self._persist_path, encoding="utf-8") as fh:
            payload = json.load(fh)
        for raw in payload.get("releases", []):
            release = _release_from_dict(raw)
            self._releases[release.release_id] = release
            if release.status not in (
                ReleaseStatus.COMPLETED,
                ReleaseStatus.ROLLED_BACK,
            ):
                self._active_index[(release.service, release.environment)] = (
                    release.release_id
                )
        for d in payload.get("decisions", []):
            self._decisions[(d["release_id"], d["request_id"])] = d["outcome"]
        for h in payload.get("history", []):
            self._history.append(
                HistoryEntry(
                    release_id=h["release_id"],
                    service=h["service"],
                    environment=h["environment"],
                    old_version=h["old_version"],
                    new_version=h["new_version"],
                    final_status=ReleaseStatus(h["final_status"]),
                    reason=h["reason"],
                    policy_snapshot=h["policy_snapshot"],
                    finished_at=h["finished_at"],
                )
            )


def _release_to_dict(r: Release) -> dict:
    return {
        "release_id": r.release_id,
        "service": r.service,
        "environment": r.environment,
        "old_version": r.old_version,
        "new_version": r.new_version,
        "status": r.status.value,
        "policy_version": r.policy_version,
        "steps": [
            {
                "new_version_percent": s.new_version_percent,
                "observe_minutes": s.observe_minutes,
            }
            for s in r.steps
        ],
        "current_step_index": r.current_step_index,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
        "finish_reason": r.finish_reason,
        "observations": {
            str(idx): [asdict(p) for p in points]
            for idx, points in r.observations.items()
        },
    }


def _release_from_dict(raw: dict) -> Release:
    from canary.models import CanaryStep

    return Release(
        release_id=raw["release_id"],
        service=raw["service"],
        environment=raw["environment"],
        old_version=raw["old_version"],
        new_version=raw["new_version"],
        status=ReleaseStatus(raw["status"]),
        policy_version=raw["policy_version"],
        steps=[
            CanaryStep(
                new_version_percent=s["new_version_percent"],
                observe_minutes=s.get("observe_minutes", 5),
            )
            for s in raw["steps"]
        ],
        current_step_index=raw["current_step_index"],
        created_at=raw.get("created_at", ""),
        updated_at=raw.get("updated_at", ""),
        finish_reason=raw.get("finish_reason", ""),
        observations={
            int(idx): [
                ObservationPoint(
                    minute_offset=p["minute_offset"],
                    total_requests=p["total_requests"],
                    failed_requests=p["failed_requests"],
                )
                for p in points
            ]
            for idx, points in raw.get("observations", {}).items()
        },
    )
