# ADR-005: HTTP Caller Authentication

## Status

Accepted for the Milestone 11 authentication slice.

## Context

The local HTTP preview has one implicit owner. A multi-user service must verify
the caller before creating or accessing sessions, queries, results, events, or
cancellation. The design must accommodate OIDC/JWT later without coupling
session and query logic to one credential format.

Engine credentials and HTTP caller credentials solve different problems. Engine
credentials authorize qcli to contact Trino, Databricks, or Snowflake. HTTP
caller credentials identify who may ask qcli to use a configured target. They
remain in separate files and abstractions.

## Decision

HTTP authentication uses an `Authenticator` interface. Every provider returns
the same `AuthenticatedPrincipal` containing:

- a stable principal ID;
- an allowed-target set;
- a maximum session count;
- a maximum concurrent-query count.

The first provider accepts opaque API keys:

```text
qcli_k1_<key-id>_<256-bit-random-secret>
```

Clients send the key through `Authorization: Bearer`. The public key ID selects
a record; the random secret is verified against an Argon2id hash. Unknown key
IDs perform a dummy hash verification to reduce identifier-based timing
differences. Raw keys are displayed once and are never stored by qcli.

The authentication file is separate from `~/.qcli/.env`, must have mode `0600`
on Unix, and contains principal policies plus key hashes. Keys can be disabled
or assigned an RFC 3339 expiry.

Every persistent session and retained query records its owning principal.
Cross-principal resource access returns `404` to avoid disclosing resource
existence. A valid caller requesting a disallowed target receives `403`.
Missing, malformed, disabled, expired, or incorrect keys receive `401` with
`WWW-Authenticate: Bearer`.

Swagger exposes the bearer security scheme and supports its Authorize control.

## JWT/OIDC extension

A future JWT provider will implement the same `Authenticator` interface. It
will validate signature, issuer, audience, expiry, and not-before claims using
the issuer's JWKS, then map `sub`, groups, or roles into an
`AuthenticatedPrincipal`. Session ownership, target authorization, quotas, and
auditing will not change.

Deployments explicitly select accepted providers. API-key format detection will
not silently enable JWT validation.

## Security boundary

This authentication slice remains loopback-only. Non-loopback binding is not
enabled until M11 supplies an explicit TLS or trusted-proxy policy, restricted
CORS, forwarded-header trust rules, audit integration, and graceful shutdown.
Database permissions remain the final authority for data access.

Neither credentials nor SQL text belong in default audit events or errors.
