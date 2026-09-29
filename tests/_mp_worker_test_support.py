"""Shared helpers for subprocess-based multi-process tests."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

WORKER = str(Path(__file__).with_name("_mp_worker.py"))


def run_worker(state_path: str, command: str, args: dict | None = None,
               timeout: int = 30) -> dict:
    """Run one command in a fresh process; return its parsed RESULT payload."""
    proc = subprocess.run(
        [sys.executable, WORKER, state_path, command, json.dumps(args or {})],
        capture_output=True, text=True, timeout=timeout,
    )
    assert proc.returncode == 0, proc.stderr
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT:")]
    assert len(lines) == 1, proc.stdout
    return json.loads(lines[0][len("RESULT:"):])
