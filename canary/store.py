"""Local state storage shared safely across processes and restarts.

Concurrency model
-----------------
Every read-modify-write sequence runs inside a :meth:`transaction` (also
exposed as :meth:`lock`, the name the engine historically uses).  A
transaction is re-entrant within one thread; only the *outermost* level
performs cross-process work:

1. acquire the in-process re-entrant lock (serializes threads);
2. acquire an exclusive ``flock`` on a sidecar ``<state>.lock`` file
   (serializes separate processes / services / CLI scripts on this machine);
3. unconditionally reload the JSON state under that lock (the file is small),
   so no entry ever works from a snapshot another process committed later;
4. mutations only mark the in-memory state dirty;
5. on clean exit of the outermost level, dirty state is persisted once via
   ``write tmp -> fsync -> atomic rename -> fsync dir``.

A crash anywhere inside a transaction therefore leaves either the previous
committed file or the complete new one — never a half-written state file —
and the idempotency decision ledger makes a post-restart retry reproduce the
same conclusion instead of appending a duplicate one.

With no ``persist_path`` the store is pure in-memory and the same code paths
run without flock/reload/flush.
"""

from __future__ import annotations

import contextlib
import fcntl
import glob
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
from canary.policy import PromotionPolicy


class LocalStore:
    """Local state backend: releases, policy versions, decision & history ledgers."""

    def __init__(self, *, persist_path: str | None = None) -> None:
        self._thread_lock = threading.RLock()
        self._persist_path = persist_path
        self._lock_path = f"{persist_path}.lock" if persist_path else None
        self._lock_fh = open(self._lock_path, "a+") if self._lock_path else None
        self._local = threading.local()
        self._dirty = False
        self._releases: dict[str, Release] = {}
        self._active_index: dict[tuple[str, str], str] = {}
        # (release_id, request_id) -> JSON outcome envelope
        self._decisions: dict[tuple[str, str], str] = {}
        self._history: list[HistoryEntry] = []
        # version -> normalized policy snapshot dict
        self._policies: dict[int, dict] = {}
        if persist_path:
            # Acquire once just to observe any pre-existing committed state.
            with self.transaction():
                pass

    # ------------------------------------------------------------------ #
    # Transaction boundary
    # ------------------------------------------------------------------ #
    def _depth(self) -> int:
        return getattr(self._local, "depth", 0)

    @contextlib.contextmanager
    def transaction(self) -> "LocalStore":
        """Re-entrant in-process + cross-process atomic transaction."""
        top_level = self._depth() == 0
        self._local.depth = self._depth() + 1
        self._thread_lock.acquire()
        if top_level and self._lock_fh is not None:
            # flock is held per open file description and not re-entrant for
            # nested acquisitions of the same file, so it is taken only by the
            # outermost transaction of this thread; the in-process RLock above
            # still serializes every nested level across threads.
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_EX)
            # The state file is small; always reload under the lock so no entry
            # can ever act on a snapshot another process has since committed.
            if os.path.exists(self._persist_path):
                self._load()
        try:
            yield self
            if top_level and self._dirty:
                self._flush()
        finally:
            if top_level and self._lock_fh is not None:
                fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
            self._thread_lock.release()
            self._local.depth = self._depth() - 1

    def lock(self):
        """Backward-compatible alias used by the engine."""
        return self.transaction()

    def _mark_dirty(self) -> None:
        self._dirty = True

    # ---- release CRUD -------------------------------------------------
    def put_release(self, release: Release) -> None:
        with self.transaction():
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
        with self.transaction():
            return self._releases.get(release_id)

    def find_release(self, service: str, environment: str) -> Release | None:
        with self.transaction():
            rid = self._active_index.get((service, environment))
            return self._releases.get(rid) if rid else None

    # ---- idempotency ledger -------------------------------------------
    def decision_seen(self, release_id: str, request_id: str) -> str | None:
        """Return remembered outcome envelope for this request, or None."""
        if not request_id:
            return None
        with self.transaction():
            return self._decisions.get((release_id, request_id))

    def remember_decision(
        self, release_id: str, request_id: str, outcome: str
    ) -> None:
        if not request_id:
            return
        with self.transaction():
            self._decisions[(release_id, request_id)] = outcome
            self._mark_dirty()

    # ---- immutable history --------------------------------------------
    def append_history(self, entry: HistoryEntry) -> None:
        with self.transaction():
            self._history.append(entry)
            self._mark_dirty()

    def list_history(self, service: str | None = None) -> list[HistoryEntry]:
        with self.transaction():
            if service is None:
                return list(self._history)
            return [h for h in self._history if h.service == service]

    # ---- versioned policy ledger --------------------------------------
    def put_policy(self, policy: PromotionPolicy) -> None:
        with self.transaction():
            self._policies[policy.version] = policy.to_dict()
            self._mark_dirty()

    def get_policy_dict(self, version: int) -> dict | None:
        with self.transaction():
            return self._policies.get(version)

    def all_policy_dicts(self) -> list[dict]:
        with self.transaction():
            return [self._policies[v] for v in sorted(self._policies)]

    def has_policies(self) -> bool:
        with self.transaction():
            return bool(self._policies)

    # ---- (de)serialization --------------------------------------------
    def _flush(self) -> None:
        if not self._persist_path:
            return
        payload = {
            "format_version": 2,
            "releases": [_release_to_dict(r) for r in self._releases.values()],
            "policies": [self._policies[v] for v in sorted(self._policies)],
            "decisions": [
                {"release_id": k[0], "request_id": k[1], "outcome": v}
                for k, v in self._decisions.items()
            ],
            "history": [asdict(h) for h in self._history],
        }
        # One fixed tmp name per process: a crash leaves only a stale sidecar,
        # never a torn main file; the main path is swapped in atomically.
        tmp = f"{self._persist_path}.tmp.{os.getpid()}"
        # Sweep sidecars left by processes that died mid-flush; the flock we
        # hold guarantees no other live writer is touching them.
        self._sweep_stale_tmpfiles()
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._persist_path)
        self._fsync_directory(os.path.dirname(os.path.abspath(self._persist_path)))

    def _sweep_stale_tmpfiles(self) -> None:
        for leftover in glob.glob(f"{self._persist_path}.tmp.*"):
            try:
                os.remove(leftover)
            except OSError:
                pass

    @staticmethod
    def _fsync_directory(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

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
        self._releases = releases
        self._active_index = active_index
        self._policies = {}
        for raw_p in payload.get("policies", []):
            policy = PromotionPolicy.from_dict(raw_p)
            self._policies[policy.version] = policy.to_dict()
        self._decisions = {
            (d["release_id"], d["request_id"]): d["outcome"]
            for d in payload.get("decisions", [])
        }
        self._history = [_history_from_dict(h) for h in payload.get("history", [])]
        # Legacy state files (written before policies were persisted) carry no
        # policy ledger.  Seed stable-default policies for every referenced
        # version so those releases can still be evaluated after restart; this
        # mirrors the stable "missing field" defaults in canary.policy. The
        # release's own ramp steps/observe durations are preserved regardless.
        if not self._policies and self._releases:
            for version in {r.policy_version for r in self._releases.values()}:
                fallback = PromotionPolicy.from_dict({"version": version})
                self._policies[version] = fallback.to_dict()


def _history_from_dict(h: dict) -> HistoryEntry:
    """Load a history record tolerating missing legacy fields."""
    snapshot = h.get("policy_snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    return HistoryEntry(
        release_id=h.get("release_id", ""),
        service=h.get("service", ""),
        environment=h.get("environment", ""),
        old_version=h.get("old_version", ""),
        new_version=h.get("new_version", ""),
        final_status=ReleaseStatus(h["final_status"]),
        reason=h.get("reason", ""),
        policy_snapshot=snapshot,
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
        policy_version=int(raw.get("policy_version", 1)),
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
