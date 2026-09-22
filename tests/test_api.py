"""HTTP entry-point tests plus cross-entry consistency (HTTP vs engine vs disk)."""

from fastapi.testclient import TestClient

from canary.api import create_app
from canary.service import CanaryService


def _client(service=None):
    service = service or CanaryService(clock=lambda: "t")
    return TestClient(create_app(service)), service


def test_error_codes_are_distinguishable_over_http():
    client, _ = _client()
    # not found
    r = client.get("/api/releases/nope")
    assert r.status_code == 404 and r.json()["error"]["code"] == "release_not_found"

    rid = client.post(
        "/api/releases",
        json={"service": "s", "environment": "prod", "old_version": "v1", "new_version": "v2"},
    ).json()["release_id"]

    # duplicate -> release_already_exists
    r = client.post(
        "/api/releases",
        json={"service": "s", "environment": "prod", "old_version": "v1", "new_version": "v3"},
    )
    assert r.status_code == 409 and r.json()["error"]["code"] == "release_already_exists"

    client.post(f"/api/releases/{rid}/promote", json={"request_id": "p0"})
    # skipping -> step_skipping
    r = client.post(f"/api/releases/{rid}/promote", json={"step_index": 9})
    assert r.status_code == 409 and r.json()["error"]["code"] == "step_skipping"
    # window pending -> 425 observation_pending
    r = client.post(f"/api/releases/{rid}/promote", json={})
    assert r.status_code == 425 and r.json()["error"]["code"] == "observation_pending"
    # invalid payload -> 422 invalid_argument
    r = client.post(
        "/api/releases",
        json={"service": "s", "environment": "x", "old_version": "v", "new_version": "v"},
    )
    assert r.status_code == 422
    print("\nHTTP rejection codes: 404/409(release_already_exists)/409(step_skipping)/425/422")


def test_http_auto_rollback_and_history():
    client, _ = _client()
    rid = client.post(
        "/api/releases",
        json={"service": "s", "environment": "prod", "old_version": "v1", "new_version": "v2"},
    ).json()["release_id"]
    client.post(f"/api/releases/{rid}/promote", json={"request_id": "enter"})
    for m in range(5):
        r = client.post(
            f"/api/releases/{rid}/observe",
            json={"step_index": 0, "minute_offset": m,
                  "total_requests": 100, "failed_requests": 5},  # exactly 5%
        )
        assert r.status_code == 200
    r = client.post(f"/api/releases/{rid}/promote", json={"request_id": "judge"})
    body = r.json()["result"]
    assert body["rolled_back"] is True and body["to_status"] == "rolled_back"
    history = client.get("/api/history").json()["history"]
    assert len(history) == 1 and history[0]["final_status"] == "rolled_back"
    assert history[0]["policy_snapshot"]["failure_rate_threshold"] == 0.05
    print("\nHTTP boundary 5% => auto rollback; history snapshot threshold=0.05")


def test_idempotent_promote_over_http():
    client, _ = _client()
    rid = client.post(
        "/api/releases",
        json={"service": "s", "environment": "dev", "old_version": "v1", "new_version": "v2"},
    ).json()["release_id"]
    a = client.post(f"/api/releases/{rid}/promote", json={"request_id": "same"}).json()["result"]
    b = client.post(f"/api/releases/{rid}/promote", json={"request_id": "same"}).json()["result"]
    assert a == b
    print("\nHTTP duplicate promote same request_id -> identical result (idempotent)")


def test_policy_creation_and_listing():
    client, _ = _client()
    r = client.post(
        "/api/policies",
        json={"name": "strict", "steps": [5, 25, 100],
              "failure_rate_threshold": 0.01, "min_requests": 500, "observe_minutes": 10},
    )
    assert r.status_code == 200 and r.json()["version"] == 2
    bad = client.post("/api/policies", json={"name": "bad", "steps": [50, 10]})
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "invalid_policy"
    versions = client.get("/api/policies").json()["policies"]
    assert [p["version"] for p in versions] == [1, 2]
    print("\nHTTP policy versioning: v1 default + v2 strict; invalid -> invalid_policy")


def test_conclusions_consistent_across_http_engine_and_disk(tmp_path):
    """Drive over HTTP, then read via engine facade and a freshly reloaded
    on-disk store: the conclusion must be identical at every entry point."""
    path = str(tmp_path / "state.json")
    service = CanaryService(persist_path=path, clock=lambda: "t")
    client = TestClient(create_app(service))

    rid = client.post(
        "/api/releases",
        json={"service": "s", "environment": "prod", "old_version": "v1", "new_version": "v2"},
    ).json()["release_id"]
    client.post(f"/api/releases/{rid}/promote", json={"request_id": "e"})
    for m in range(5):
        client.post(
            f"/api/releases/{rid}/observe",
            json={"step_index": 0, "minute_offset": m,
                  "total_requests": 100, "failed_requests": 5},
        )
    http = client.post(f"/api/releases/{rid}/promote", json={"request_id": "j"}).json()

    # same process, engine entry point
    engine_release = service.get_release(rid)
    assert engine_release.status.value == http["result"]["to_status"] == "rolled_back"

    # new process simulation: reload persisted state from disk
    reloaded = CanaryService(persist_path=path, clock=lambda: "t")
    disk_release = reloaded.get_release(rid)
    disk_history = reloaded.history()
    assert disk_release.status.value == "rolled_back"
    assert disk_release.finish_reason == engine_release.finish_reason
    assert len(disk_history) == 1
    assert disk_history[0].final_status.value == "rolled_back"
    # idempotency ledger also persisted: same request replays even after reload
    replay = reloaded.promote(rid, request_id="j")
    assert replay.rolled_back is True
    print("\nCONSISTENCY: HTTP == engine == reloaded disk state == rolled_back")
