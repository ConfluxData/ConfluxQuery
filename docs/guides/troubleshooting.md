# Troubleshooting

## `target list` asks for an unrelated credential

Current ConfluxQuery CLI target discovery does not expand target credentials. Confirm you
are running the expected binary with `qcli --version`. Use `config check` only
when all referenced environment variables are intentionally available.

## A target always uses the original catalog/schema

Run `\status` after `USE`, `\use-catalog`, or `\use-schema`. Prefer ConfluxQuery CLI's
validated commands when the engine exposes metadata. Qualified namespace
syntax differs: on Databricks, change catalog and schema separately if Unity
Catalog rejects a nested schema.

## Databricks command reports missing `columns`

Successful `USE` and other command statements can return no result columns.
ConfluxQuery treats that as a successful command and applies reported context.
Upgrade if an older adapter attempts to deserialize every response as a rowset.

## Snowflake metadata works but rows are empty

Verify the query through the current ConfluxQuery release and inspect role, warehouse,
database, schema, row visibility, and native response decoding. Earlier client
paths could expose metadata while silently dropping decoded row data; ConfluxQuery now
fails on that inconsistency rather than returning false success.

## HTTP returns 401

- Send `Authorization: Bearer RAW_KEY`, not the stored Argon2id hash.
- Confirm key `enabled`, expiry, principal mapping, and auth-file permissions.
- For JWT, inspect issuer, audience, expiry, JWKS, subject, and group mapping.

## HTTP returns 403

The principal is authenticated but origin/target/policy is forbidden. Check
the target allowlist and exact CORS origin. Resource ownership mismatches may be
returned as 404 to avoid disclosure.

## Non-loopback HTTP fails or returns 426

Use `--trusted-proxy` and configure the directly trusted proxy to add
`x-forwarded-proto: https`. Do not send that header from an untrusted client
through a proxy that fails to replace it.

## Flight client is unauthenticated

Pass a bearer authorization header and either `qcli-target` metadata or a
session cookie with `qcli.target`. Confirm TLS URI/encryption settings match the
listener and that proxy mode is gRPC-aware.

## Result/ticket/session is not found

It may have expired, been closed, belong to another principal, or be absent
from a standalone node after restart. In cluster mode verify PostgreSQL,
object-store connectivity, shared signing key, clock/database time, and node
schema compatibility.

## Query cannot be cancelled

Check `qcli target capabilities NAME`. The adapter may only support a local
cancellation request, or the engine may already be terminal. Use the native
query ID in warehouse diagnostics when available.

## Node is live but not ready

Readiness returns 503 during draining. Check shutdown signals, PostgreSQL
registration/heartbeat, state schema compatibility, object storage, signing
key permissions, and configured resource limits before reintroducing traffic.

## Collect a safe incident bundle

Capture version, deployment digest, target name/engine (not credentials), qcli
and native query IDs, principal ID, session ID/version, timestamps, stable error
code, listener mode, node ID, and redacted audit lines. Do not collect raw
tokens, passwords, private keys, auth files, or SQL unless the incident process
explicitly authorizes protected SQL handling.
