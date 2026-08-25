# Security and identity

qcli separates **caller identity** from **warehouse credentials**. A client
authenticates to the gateway; the selected target's server-side credential
provider authenticates to Trino, Databricks, or Snowflake.

## Caller authentication

### Opaque API keys

`qcli auth key create ID` emits a raw key once and an Argon2id hash. The auth
file stores only the hash. Keys can be disabled or assigned `expires_at`.

### JWT/OIDC

OIDC validates signature, issuer, audience, expiry, subject, and configured
group mappings. JWKS can rotate without restarting query execution. API keys
and OIDC can coexist during migration.

### mTLS

Flight SQL can require a client CA. Verified certificate identity participates
in principal/resource ownership; TLS termination must remain direct when qcli
is responsible for mTLS.

## Authorization

A principal declares:

- Allowed target names.
- Maximum live sessions.
- Maximum concurrent queries.

Every session, query, result, prepared handle, ticket, and ingestion operation
is owner-bound. Authorization is enforced at creation and retrieval, including
across cluster nodes.

## Transport modes

| Listener | Loopback | Non-loopback |
|---|---|---|
| HTTP | Allowed for preview | Requires `--trusted-proxy` and forwarded HTTPS |
| Flight SQL | Requires authentication | Requires direct TLS or `--flight-trusted-proxy` |
| Flight mTLS | Direct TLS | Direct TLS with client CA |

Trusted-proxy mode is an explicit deployment trust decision. qcli rejects
missing or non-HTTPS forwarded transport rather than guessing.

## Secrets

- Put environment references, not raw secrets, in target files when practical.
- Keep config, auth, OIDC, TLS keys, and signing keys mode `0600` or projected
  read-only by an orchestrator.
- Use `QCLI_FLIGHT_SIGNING_KEY` for a protected path, not signing-key bytes.
- Never log raw API keys, bearer tokens, SQL containing secrets, or warehouse
  credentials.
- Rotate server credentials through provider/config replacement and validate
  new connections before retiring the old credential.

## Audit and telemetry

Audit records include action, outcome, principal, target, session/query
correlation, and stable failure classification. SQL text and credentials are
omitted from default audit events. Security telemetry covers authentication
failures, quota rejections, proxy enforcement, and ownership denial.

See [enterprise identity](../enterprise-identity-and-transport.md) for full
configuration and rotation procedures.
