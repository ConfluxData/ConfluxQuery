#!/usr/bin/env python3
"""Credential-free packaged HTTP query profile using only the Python stdlib."""

import json
import os
import time
import urllib.request

BASE = os.getenv("QCLI_HTTP_URL", "http://127.0.0.1:18089")
TOKEN = os.environ["QCLI_HTTP_TOKEN"]
TARGET = os.getenv("QCLI_HTTP_TARGET", "demo")
QUERY = os.getenv("QCLI_HTTP_QUERY", "select * from sample")
EXPECTED_ROWS = int(os.getenv("QCLI_HTTP_EXPECTED_ROWS", "2"))


def request(method: str, path: str, body=None):
    data = None if body is None else json.dumps(body).encode()
    call = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(call, timeout=10) as response:
        return json.load(response)


query = request("POST", "/v1/queries", {"target": TARGET, "sql": QUERY})
query_id = query["id"]
for _ in range(100):
    status = request("GET", f"/v1/queries/{query_id}")
    if status["state"] in {"completed", "failed", "cancelled"}:
        break
    time.sleep(0.05)
else:
    raise RuntimeError("HTTP query did not terminate")
if status["state"] != "completed" or status["rows"] != EXPECTED_ROWS:
    raise RuntimeError(f"unexpected query status: {status}")
results = request("GET", f"/v1/queries/{query_id}/results?limit=10")
if not isinstance(results, list) or len(results) != EXPECTED_ROWS:
    raise RuntimeError(f"unexpected HTTP results: {results}")
print("qcli HTTP profile passed")
