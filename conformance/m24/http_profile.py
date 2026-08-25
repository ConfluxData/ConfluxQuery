#!/usr/bin/env python3
"""Credential-free packaged HTTP query profile using only the Python stdlib."""

import json
import os
import time
import urllib.request

BASE = os.getenv("QCLI_HTTP_URL", "http://127.0.0.1:18089")
TOKEN = os.environ["QCLI_HTTP_TOKEN"]


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


query = request("POST", "/v1/queries", {"target": "demo", "sql": "select * from sample"})
query_id = query["id"]
for _ in range(100):
    status = request("GET", f"/v1/queries/{query_id}")
    if status["state"] in {"completed", "failed", "cancelled"}:
        break
    time.sleep(0.05)
else:
    raise RuntimeError("HTTP query did not terminate")
if status["state"] != "completed" or status["rows"] != 2:
    raise RuntimeError(f"unexpected query status: {status}")
results = request("GET", f"/v1/queries/{query_id}/results?limit=10")
if not isinstance(results, list) or len(results) != 2:
    raise RuntimeError(f"unexpected HTTP results: {results}")
print("qcli HTTP profile passed")
