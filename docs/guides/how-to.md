# How-to guides

This page is an index of task recipes. Follow linked concept/reference pages
when you need the underlying contract.

## Change target during a terminal session

```text
\targets
\use databricks-dev
\status
```

The destination is validated first; failure leaves the current target intact.

## Change catalog and schema

```text
\catalogs
\use-catalog hive
\schemas
\use-schema analytics
\status
```

Use separate operations on engines whose namespace grammar rejects qualified
schema names.

## Override a session property

```text
\set query_timeout 2m
\set output_format vertical
\properties
```

HTTP uses a versioned PATCH to `/v1/sessions/{id}/options`; Flight SQL uses
`SetSessionOptions`.

## Cancel work

- CLI: press Ctrl-C while the query runs.
- HTTP: `POST /v1/queries/{query_id}/cancel`.
- Flight SQL: use the client's cancel API/`CancelFlightInfo` support.

Inspect adapter/client capabilities before promising hard cancellation timing.

## Use Swagger

```bash
qcli --config targets.env serve
```

Open `http://127.0.0.1:8088/docs/`. In authenticated mode, use Swagger's
Authorize control with the raw bearer key. The generated contract is at
`/openapi.json`.

## Add a browser client

```bash
qcli serve --auth-file auth.toml \
  --cors-origin https://analytics.example.com
```

List every exact allowed origin separately. Put ConfluxQuery Gateway behind the documented TLS
proxy before non-loopback access.

## Enable OIDC

1. Register the gateway audience with the identity provider.
2. Configure issuer, audience, JWKS, subject claim, and group-to-target policy.
3. Start with `--oidc-file oidc.toml`; API keys may remain enabled during
   migration.
4. Test expired, wrong-audience, wrong-issuer, unknown-group, and JWKS-rotation
   cases before production.

See [enterprise identity](../enterprise-identity-and-transport.md).

## Enable Flight mTLS

```bash
qcli serve --auth-file auth.toml \
  --flight-bind 0.0.0.0:32010 \
  --flight-tls-cert server-chain.pem \
  --flight-tls-key server-key.pem \
  --flight-tls-client-ca client-ca.pem
```

Distribute client certificates through organizational PKI and map verified
identity according to policy. Test rotation with overlapping CA trust.

## Enable cluster mode

1. Create a dedicated PostgreSQL database and least-privilege credential.
2. Provision an S3-compatible bucket/prefix with encryption and lifecycle.
3. Generate one protected 32-byte Flight signing-key file for all nodes.
4. Set the four `QCLI_*` cluster environment variables.
5. Start nodes, verify `/health/ready`, then put them behind HTTP and gRPC-aware
   load balancers.
6. Drain one node and prove sessions/results remain accessible through another.

See [high availability](../high-availability.md) and
[operations](../operations.md).

## Upgrade or roll back

Read the release notes and state compatibility first. Back up PostgreSQL,
retain the prior image/binary, drain nodes one at a time, run smoke/client
profiles, and only then complete rollout. Rollback is a binary/deployment
rollback; do not reverse a database migration unless the release explicitly
documents that operation.
