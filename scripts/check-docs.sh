#!/usr/bin/env bash
set -euo pipefail

python3 -m mkdocs build --strict
if rg --quiet ':material-[a-z-]+:' site/index.html; then
  echo "Material icon shortcode was not rendered on the documentation homepage" >&2
  exit 1
fi
cargo run --quiet --locked -- --help > /tmp/qcli-documented-help.txt
python3 scripts/check-cli-docs.py \
  /tmp/qcli-documented-help.txt \
  docs/reference/cli.md \
  docs/reference/repl.md \
  docs/reference/configuration.md \
  crates/qcli-config/src/lib.rs \
  crates/qcli-repl/src/lib.rs
python3 -m py_compile conformance/m24/http_profile.py conformance/m19/python/profile.py
python3 scripts/check-branding.py
