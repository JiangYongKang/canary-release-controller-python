"""本地运行入口：uv run fastapi dev main.py 或 uv run python main.py"""

from __future__ import annotations

from canary.api import create_app
from canary.clock import Clock
from canary.metrics import MetricsRegistry
from canary.service import ReleaseService
from canary.store import Store


def build_service() -> ReleaseService:
    """组装默认的本地服务（内存状态 + 真实时钟）。"""
    store = Store()
    metrics = MetricsRegistry(Clock())
    service = ReleaseService(store, metrics, Clock())
    return service


app = create_app(build_service())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
