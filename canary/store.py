"""Local state storage shared safely across processes and restarts.

A single JSON state file holds releases, the idempotency decision ledger, the
immutable history ledger and the versioned policy set, so every entry point on
the same machine (long-running service, CLI, background script) sees exactly
the same state.

Cross-process transaction semantics
-----------------------------------
:meth:`lock()` yields one *transaction* that combines:

* an in-process re-entrant lock (serializes threads inside one process), and
* an exclusive ``flock`` on a sibling lock file (serializes processes).

On entering the outermost transaction the file lock is taken **and the state
file is reloaded from disk**, so a process always observes writes committed by
other processes, even ones that happened while it was idle.  Mutations only
mark the in-memory state dirty; on leaving the outermost transaction the whole
state is written **once**, atomically (unique temp file + ``fsync`` +
``os.replace`` + directory ``fsync``).  A process killed mid-transaction can
therefore never leave a half-written state file: either the previous complete
snapshot survives, or the new complete snapshot does.

When ``persist_path`` is unset the store is a plain in-memory backend: the same
transaction rules apply, minus file locking and disk I/O.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
import threading
from dataclasses import asdict

from canary.models import (
    HistoryEntry,
    ObservationPoint,
    Release,
    ReleaseStatus,
)


class _Transaction:
    """Re-entrant cross-process transaction handed out by :meth:`LocalStore.lock`."""

    def __init__(self, store: "LocalStore") -> None:
        self._store = store

    def __enter__(self) -> "_Transaction":
        self._store._tx_enter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._store._tx_exit(commit=exc_type is None)


class LocalStore:
    """File-backed local state backend, safe for concurrent multi-process use."""

    def __init__(self, *, persist_path: str | None = None) -> None:
        self._thread_lock = threading.RLock()
        self._persist_path = persist_path
        self._lock_path = persist_path + ".lock" if persist_path else None
        self._lock_fd: int | None = None
        self._local = threading.local()
        self._dirty = False
        self._releases: dict[str, Release] = {}
        self._active_index: dict[tuple[str, str], str] = {}
        # (release_id, request_id) -> serialized outcome envelope
        self._decisions: dict[tuple[str, str], str] = {}
        self._history: list[HistoryEntry] = []
        # policy version -> raw policy snapshot dict (PromotionPolicy.to_dict)
        self._policies: dict[int, dict] = {}
        if persist_path and os.path.exists(persist_path):
            self._load()

    # ------------------------------------------------------------------ #
    # Transaction machinery
    # ------------------------------------------------------------------ #
    def lock(self) -> _Transaction:
        return _Transaction(self)

    @property
    def _depth(self) -> int:
        return getattr(self._local, "depth", 0)

    @_depth.setter
    def _depth(self, value: int) -> None:
        self._local.depth = value

    def _tx_enter(self) -> None:
        if self._depth == 0:
            self._thread_lock.acquire()
            self._local.locked = True
            try:
                if self._persist_path:
                    if self._lock_fd is None:
                        self._lock_fd = os.open(
                            self._lock_path, os.O_RDWR | os.O_CREAT, 0o600
                        )
                    # Exclusive across every process that shares the state file.
                    fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
                    # Pick up everything other processes committed since our last tx.
                    if os.path.exists(self._persist_path):
                        self._load()
                self._dirty = False
            except BaseException:
                # Acquire failed (e.g. a corrupt state file): release what we
                # hold before propagating, so no thread/file lock leaks.
                self._release_locks()
                self._local.locked = False
                raise
        self._depth += 1

    def _tx_exit(self, *, commit: bool) -> None:
        self._depth -= 1
        if self._depth > 0:
            return
        try:
            if not getattr(self._local, "locked", False):
                return
            if commit and self._dirty:
                self._flush()
            elif not commit and self._persist_path:
                # The transaction raised mid-read-modify-write: discard the
                # half-applied in-memory state.  Nothing was flushed, and the
                # next outer transaction reloads the last committed snapshot.
                if os.path.exists(self._persist_path):
                    self._load()
                else:
                    self._reset_blank()
        finally:
            self._dirty = False
            self._release_locks()

    def _release_locks(self) -> None:
        if self._persist_path and self._lock_fd is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        self._thread_lock.release()

    def _mark_dirty(self) -> None:
        self._dirty = True

    # ---- release CRUD -------------------------------------------------
    def put_release(self, release: Release) -> None:
        with self.lock():
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
            self._mark_dirty()

    def get_release(self, release_id: str) -> Release | None:
        with self.lock():
            return self._releases.get(release_id)

    def find_release(self, service: str, environment: str) -> Release | None:
        with self.lock():
            rid = self._active_index.get((service, environment))
            return self._releases.get(rid) if rid else None

    # ---- idempotency ledger -------------------------------------------
    def decision_seen(self, release_id: str, request_id: str) -> str | None:
        """Return remembered outcome for this request, or None."""
        if not request_id:
            return None
        with self.lock():
            return self._decisions.get((release_id, request_id))

    def remember_decision(
        self, release_id: str, request_id: str, outcome: str
    ) -> None:
        if not request_id:
            return
        with self.lock():
            self._decisions[(release_id, request_id)] = outcome
            self._mark_dirty()

    # ---- immutable history --------------------------------------------
    def append_history(self, entry: HistoryEntry) -> None:
        with self.lock():
            self._history.append(entry)
            self._mark_dirty()

    def list_history(self, service: str | None = None) -> list[HistoryEntry]:
        with self.lock():
            if service is None:
                return list(self._history)
            return [h for h in self._history if h.service == service]

    # ---- versioned policy set -----------------------------------------
    def put_policy(self, raw: dict) -> None:
        with self.lock():
            self._policies[int(raw["version"])] = dict(raw)
            self._mark_dirty()

    def get_policy(self, version: int) -> dict | None:
        with self.lock():
            raw = self._policies.get(version)
            return dict(raw) if raw is not None else None

    def all_policies(self) -> list[dict]:
        with self.lock():
            return [dict(self._policies[v]) for v in sorted(self._policies)]

    # ---- (de)serialization --------------------------------------------
    def _reset_blank(self) -> None:
        self._releases = {}
        self._active_index = {}
        self._decisions = {}
        self._history = []
        self._policies = {}

    def _flush(self) -> None:
        if not self._persist_path:
            return
        payload = {
            "format_version": 1,
            "releases": [_release_to_dict(r) for r in self._releases.values()],
            "decisions": [
                {"release_id": k[0], "request_id": k[1], "outcome": v}
                for k, v in self._decisions.items()
            ],
            "history": [asdict(h) for h in self._history],
            "policies": [dict(raw) for raw in self._policies.values()],
        }
        directory = os.path.dirname(os.path.abspath(self._persist_path))
        fd, tmp = tempfile.mkstemp(
            dir=directory, prefix=".canary-state.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._persist_path)
            # Make the rename itself durable across machine crashes.
            dfd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp)
            raise

    def _load(self) -> None:
        with open(self._persist_path, encoding="utf-8") as fh:
            payload = json.load(fh)
        releases: dict[str, Release] = {}
        active_index: dict[tuple[str, str], str] = {}
        for raw in payload.get("releases", []):
            release = _release_from_dict(raw)
            releases[release.release_id] = release
            if release.status not in (
                ReleaseStatus.COMPLETED,
                ReleaseStatus.ROLLED_BACK,
            ):
                active_index[(release.service, release.environment)] = (
                    release.release_id
                )
        decisions: dict[tuple[str, str], str] = {}
        for d in payload.get("decisions", []):
            decisions[(d["release_id"], d["request_id"])] = d["outcome"]
        history = [_history_from_dict(h) for h in payload.get("history", [])]
        policies = {
            int(p["version"]): dict(p) for p in payload.get("policies", [])
        }
        self._reset_blank()
        self._releases = releases
        self._active_index = active_index
        self._decisions = decisions
        self._history = history
        self._policies = policies


def _history_from_dict(h: dict) -> HistoryEntry:
    return HistoryEntry(
        release_id=h["release_id"],
        service=h["service"],
        environment=h["environment"],
        old_version=h["old_version"],
        new_version=h["new_version"],
        final_status=ReleaseStatus(h["final_status"]),
        reason=h.get("reason", ""),
        policy_snapshot=h.get("policy_snapshot", {}),
        finished_at=h.get("finished_at", ""),
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
