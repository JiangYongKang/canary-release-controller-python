#!/usr/bin/env python3
"""Local end-to-end demo over the HTTP entry point.

Run:  uv run python scripts/demo.py
Exercises: policy versioning -> registration -> ramp -> healthy completion,
then a second release that auto-rolls back at the threshold boundary, plus
idempotent duplicate submissions and the immutable history ledger.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow ``python scripts/demo.py`` from any working directory without an install.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from canary.api import create_app
from canary.service import CanaryService

logging.basicConfig(level=logging.INFO, format="  %(message)s", stream=sys.stdout)


def banner(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    client = TestClient(create_app(CanaryService(clock=lambda: "fixed-clock")))

    banner("1. create a strict canary policy v2")
    policy = client.post(
        "/api/policies",
        json={
            "name": "strict",
            "steps": [20, 60, 100],
            "failure_rate_threshold": 0.05,
            "min_requests": 100,
            "observe_minutes": 2,
        },
    ).json()
    print("policy:", policy["version"], policy["steps"], "threshold", policy["failure_rate_threshold"])

    banner("2. healthy release ramps 0% -> 20% -> 60% -> 100% -> COMPLETED")
    rid = client.post(
        "/api/releases",
        json={"service": "checkout", "environment": "prod",
              "old_version": "v1.0", "new_version": "v1.1",
              "policy_version": policy["version"]},
    ).json()["release_id"]

    print(client.post(f"/api/releases/{rid}/promote", json={"request_id": "e"}).json()["result"])
    for step, _ in enumerate(policy["steps"]):
        for minute in range(2):
            client.post(
                f"/api/releases/{rid}/observe",
                json={"step_index": step, "minute_offset": minute,
                      "total_requests": 200, "failed_requests": 0},
            )
        result = client.post(
            f"/api/releases/{rid}/promote", json={"request_id": f"p{step}"}
        ).json()["result"]
        print(f"after step {step}:", result["to_status"], result["new_version_percent"], "%")
        if result["to_status"] == "completed":
            break

    banner("3. duplicate submissions are idempotent (same conclusion, no error)")
    replay = client.post(f"/api/releases/{rid}/promote", json={"request_id": f"p{step}"}).json()
    print("replay:", replay["result"]["to_status"], replay["result"]["reason"])
    rejected = client.post(f"/api/releases/{rid}/promote", json={"request_id": "new-one"})
    print("new request on finished release ->", rejected.status_code, rejected.json()["error"]["code"])

    banner("4. unhealthy release auto-rolls back at boundary (fail_rate == 5%)")
    rid2 = client.post(
        "/api/releases",
        json={"service": "payments", "environment": "staging",
              "old_version": "v2.0", "new_version": "v2.1",
              "policy_version": policy["version"]},
    ).json()["release_id"]
    client.post(f"/api/releases/{rid2}/promote", json={"request_id": "e2"})
    for minute in range(2):
        client.post(
            f"/api/releases/{rid2}/observe",
            json={"step_index": 0, "minute_offset": minute,
                  "total_requests": 100, "failed_requests": 5},
        )
    judged = client.post(f"/api/releases/{rid2}/promote", json={"request_id": "j2"}).json()
    print("judgement:", judged["result"])
    rb_replay = client.post(f"/api/releases/{rid2}/rollback", json={"request_id": "j2"})
    print("same request_id used for rollback ->", rb_replay.status_code,
          rb_replay.json().get("error", {}).get("code", "n/a"))

    banner("5. history ledger keeps conclusions + the policy snapshot used")
    for h in client.get("/api/history").json()["history"]:
        print(h["service"], h["new_version"], "->", h["final_status"],
              "| threshold at decision time:",
              h["policy_snapshot"]["failure_rate_threshold"])

    print("\nDEMO DONE")


if __name__ == "__main__":
    main()
