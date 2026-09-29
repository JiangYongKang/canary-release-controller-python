"""Compatibility entry point: exposes the FastAPI app at module root.

When launched as a resident service (``uvicorn main:app``) it defaults to a
file-backed service so that CLI scripts and background jobs in separate
processes share the exact same local state, and a process/machine restart
continues unfinished releases.  The state file defaults to
``./canary-state.json`` and can be overridden with the ``CANARY_STATE``
environment variable.  ``CANARY_STATE=memory`` forces the in-memory mode.
"""

from __future__ import annotations

import os

from canary.api import create_app
from canary.service import CanaryService

_state = os.environ.get("CANARY_STATE", "canary-state.json")
app = (
    create_app()
    if _state == "memory"
    else create_app(service=CanaryService(persist_path=_state))
)

__all__ = ["app"]
