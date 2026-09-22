"""Compatibility entry point: exposes the FastAPI app at module root."""

from canary.api import app

__all__ = ["app"]
