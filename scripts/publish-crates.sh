#!/usr/bin/env bash
set -euo pipefail

: "${CARGO_REGISTRY_TOKEN:?CARGO_REGISTRY_TOKEN is required}"

packages=(
  qcli-auth
  qcli-config
  qcli-driver-api
  qcli-output
  qcli-driver-demo
  qcli-metadata
  qcli-core
  qcli-driver-conformance
  qcli-driver-databricks
  qcli-driver-snowflake
  qcli-driver-trino
  qcli-repl
  qcli-http
  qcli
)

for package in "${packages[@]}"; do
  for attempt in 1 2 3 4 5; do
    if cargo publish --locked -p "$package"; then
      break
    fi
    if [[ "$attempt" == 5 ]]; then
      echo "failed to publish $package after registry-index retries" >&2
      exit 1
    fi
    sleep $((attempt * 15))
  done
done
