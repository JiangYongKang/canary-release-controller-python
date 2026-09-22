"""本地状态存储：模拟制品库/环境/发布单/策略的持久化。

使用一把可重入锁保证多线程（并发 HTTP 请求）下的原子性；
可选 JSON 文件落盘，便于本地手动验证与重启复现。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import RLock
from typing import Any

from canary.models import Release, release_from_dict, release_to_dict


class Store:
    """线程安全的本地存储。

    发布单与策略保存在内存字典中；给定 ``path`` 时每次写入都会原子落盘
    （先写临时文件再 rename），可通过 :meth:`load` 在启动时恢复。
    """

    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        self._lock = RLock()
        self._path = Path(path) if path else None
        self._releases: dict[str, Release] = {}
        self._policies: dict[str, dict[str, Any]] = {}
        if self._path and self._path.exists():
            self.load()

    # ---- 发布单 ----
    def put_release(self, release: Release) -> None:
        """写入/更新发布单并按需要落盘。"""
        with self._lock:
            self._releases[release.release_id] = release
            self._persist_locked()

    def get_release(self, release_id: str) -> Release | None:
        """按 ID 读取发布单。"""
        with self._lock:
            return self._releases.get(release_id)

    def find_active(self, service: str, environment: str) -> Release | None:
        """查找某服务+环境下唯一的非终态发布单。"""
        with self._lock:
            for release in self._releases.values():
                if (
                    release.service == service
                    and release.environment == environment
                    and not release.status.terminal
                ):
                    return release
            return None

    def list_releases(self) -> list[Release]:
        """列出全部发布单（按创建顺序）。"""
        with self._lock:
            return list(self._releases.values())

    # ---- 策略 ----
    def put_policy(self, name: str, doc: dict[str, Any]) -> None:
        """写入策略文档（含全部历史版本）。"""
        with self._lock:
            self._policies[name] = doc
            self._persist_locked()

    def get_policy(self, name: str) -> dict[str, Any] | None:
        """读取策略文档。"""
        with self._lock:
            return self._policies.get(name)

    def list_policies(self) -> dict[str, dict[str, Any]]:
        """读取全部策略文档（副本）。"""
        with self._lock:
            return {name: dict(doc) for name, doc in self._policies.items()}

    def lock(self) -> RLock:
        """暴露存储锁，供服务层做复合原子操作。"""
        return self._lock

    # ---- 序列化 ----
    def snapshot(self) -> dict[str, Any]:
        """导出可 JSON 序列化快照。"""
        with self._lock:
            return {
                "releases": [
                    release_to_dict(r) for r in self._releases.values()
                ],
                "policies": {
                    name: json.loads(json.dumps(doc))
                    for name, doc in self._policies.items()
                },
            }

    def flush(self) -> None:
        """强制落盘。"""
        with self._lock:
            self._persist_locked()

    def load(self) -> None:
        """从磁盘恢复状态（文件不存在则为空状态）。"""
        if not self._path or not self._path.exists():
            return
        with self._lock:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            self._releases = {
                item["release_id"]: release_from_dict(item)
                for item in data.get("releases", [])
            }
            self._policies = dict(data.get("policies", {}))

    def _persist_locked(self) -> None:
        """调用方必须已持有锁。"""
        if self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.snapshot(), ensure_ascii=False, indent=2)
        with NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self._path.parent,
            prefix=".canary-",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp.write(payload)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, self._path)
