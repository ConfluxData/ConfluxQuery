#!/usr/bin/env bash
set -euo pipefail

target="${1:?target is required}"
version="${2:?version is required}"
name="qcli-${version}-${target}"
stage="dist/${name}"

rm -rf dist
mkdir -p "${stage}/completions" "${stage}/docs" "${stage}/deploy"
binary="target/${target}/release/qcli"
if [[ ! -x "$binary" ]]; then binary="target/release/qcli"; fi
[[ -x "$binary" ]] || { echo "release binary not found for ${target}" >&2; exit 1; }
cp "$binary" "${stage}/qcli"
cp README.md "${stage}/README.md"
cp LICENSE CHANGELOG.md "${stage}/"
cp docs/connectivity-compatibility.md docs/enterprise-identity-and-transport.md \
  docs/high-availability.md docs/releasing.md docs/supported-platforms.md "${stage}/docs/"
cp docs/operations.md docs/unified-connectivity-release.md "${stage}/docs/"
cp -R deploy/kubernetes deploy/systemd "${stage}/deploy/"
sed "s/qcli 0\\.1\\.0/qcli ${version}/" packaging/qcli.1 > "${stage}/qcli.1"
cp packaging/completions/* "${stage}/completions/"
tar -C dist -czf "dist/${name}.tar.gz" "${name}"
rm -rf "${stage}"
