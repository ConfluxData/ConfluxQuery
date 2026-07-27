#!/usr/bin/env bash
set -euo pipefail

target="${1:?target is required}"
version="${2:?version is required}"
name="qcli-${version}-${target}"
stage="dist/${name}"

rm -rf dist
mkdir -p "${stage}/completions"
binary="target/${target}/release/qcli"
if [[ ! -x "$binary" ]]; then binary="target/release/qcli"; fi
[[ -x "$binary" ]] || { echo "release binary not found for ${target}" >&2; exit 1; }
cp "$binary" "${stage}/qcli"
cp README.md "${stage}/README.md"
sed "s/qcli 0\\.1\\.0/qcli ${version}/" packaging/qcli.1 > "${stage}/qcli.1"
cp packaging/completions/* "${stage}/completions/"
tar -C dist -czf "dist/${name}.tar.gz" "${name}"
rm -rf "${stage}"
