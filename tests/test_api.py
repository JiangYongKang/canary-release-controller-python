"""HTTP 入口单测：各入口（REST/服务层）结论一致，非法操作拒绝原因可区分。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from canary.api import create_app
from canary.clock import FrozenClock
from canary.metrics import MetricsRegistry
from canary.service import ReleaseService
from canary.store import Store

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("test.api")


@pytest.fixture()
def client() -> TestClient:
    clock = FrozenClock(1_000.0)
    service = ReleaseService(Store(), MetricsRegistry(clock), clock)
    return TestClient(create_app(service))


def register(client: TestClient, rid: str = "r1", **overrides) -> dict:
    payload = {
        "release_id": rid,
        "service": "svc",
        "environment": "prod",
        "old_version": "v1",
        "new_version": "v2",
    }
    payload.update(overrides)
    resp = client.post("/releases", json=payload)
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_register_and_promote_flow(client: TestClient) -> None:
    register(client)
    resp = client.post("/releases/r1/promote", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "canary" and body["stage_index"] == 0
    assert body["current_weight"] == 10
    log.info("HTTP 迁移: PENDING -> canary@0 weight=10, body=%s", body)


def test_duplicate_register_rejected_with_distinct_code(client: TestClient) -> None:
    register(client)
    resp = client.post("/releases", json={
        "release_id": "r1", "service": "svc", "environment": "prod",
        "old_version": "v1", "new_version": "v2",
    })
    assert resp.status_code == 409
    assert resp.json()["error"] == "release_already_exists"
    log.info("重复登记被拒绝: %s", resp.json())


def test_not_found_code(client: TestClient) -> None:
    resp = client.post("/releases/missing/promote", json={})
    assert resp.status_code == 409
    assert resp.json()["error"] == "release_not_found"


def test_skip_promote_rejected(client: TestClient) -> None:
    register(client)
    resp = client.post("/releases/r1/promote", json={"target_stage": 2})
    assert resp.status_code == 409
    assert resp.json()["error"] == "stage_skipped"
    log.info("越级推进被拒绝: %s", resp.json())


def test_promote_finished_then_repeated_rejected(client: TestClient) -> None:
    register(client)
    for _ in range(4):
        client.post("/releases/r1/promote", json={})
    resp = client.post("/releases/r1/promote", json={})
    assert resp.status_code == 409
    assert resp.json()["error"] == "release_already_finished"
    log.info("终态重复推进被拒绝: %s", resp.json())


def test_double_rollback_rejected(client: TestClient) -> None:
    register(client)
    client.post("/releases/r1/promote", json={})
    assert client.post("/releases/r1/rollback", json={"reason": "x"}).status_code == 200
    resp = client.post("/releases/r1/rollback", json={})
    assert resp.status_code == 409
    assert resp.json()["error"] == "release_already_finished"


def test_idempotent_promote_via_http(client: TestClient) -> None:
    register(client)
    r1 = client.post("/releases/r1/promote", json={"idempotency_key": "k"})
    r2 = client.post("/releases/r1/promote", json={"idempotency_key": "k"})
    assert r1.json()["stage_index"] == r2.json()["stage_index"] == 0
    events = client.get("/releases/r1").json()["events"]
    assert sum(1 for e in events if e["action"] == "promote") == 1


def test_auto_check_does_not_rollback_before_window(client: TestClient) -> None:
    register(client)
    client.post("/releases/r1/promote", json={})
    for _ in range(20):
        client.post("/metrics", json={
            "service": "svc", "environment": "prod", "success": False,
        })
    resp = client.post("/releases/r1/auto-check")
    body = resp.json()
    assert body["release"]["status"] == "canary"
    assert body["verdict"]["observation_complete"] is False
    log.info("窗口未满 auto-check 判定: %s", body["verdict"]["detail"]["decision"])


def test_history_endpoint_reproduces(client: TestClient) -> None:
    register(client)
    client.post("/releases/r1/promote", json={})
    client.post("/releases/r1/auto-check")
    resp = client.get("/releases/r1/history")
    assert resp.status_code == 200
    body = resp.json()
    assert body["reproduced"] is True
    log.info("历史复现: %s", {k: body[k] for k in ("reproduced", "final_status", "policy_version")})


def test_policy_versioning_via_http(client: TestClient) -> None:
    resp = client.post("/policies/default/versions", json={
        "definition": {"failure_rate_threshold": 0.01},
    })
    assert resp.status_code == 200 and resp.json()["version"]["version"] == 2
    got = client.get("/policies/default/versions/2")
    # 缺省迁移：只给阈值，其它字段按 v1 缺省补齐
    assert got.json()["definition"]["min_samples"] == 10
    assert got.json()["definition"]["failure_rate_threshold"] == 0.01
