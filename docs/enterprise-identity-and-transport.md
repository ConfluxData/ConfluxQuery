# Enterprise Identity and Transport

qcli accepts API keys and validated OIDC access tokens through one authentication
boundary shared by HTTP and Flight SQL. A token becomes a qcli principal only
after its signature, key ID, signing algorithm, issuer, audience, expiry, and
subject pass validation. Group claims then determine target access and quotas.

## OIDC configuration

Use `examples/milestone-22-oidc.toml` as the policy template. The configured
JWKS is a local JSON file so production deployments can obtain keys through
their approved discovery, sidecar, or configuration-management path. Replace
the file atomically during issuer key rotation. qcli detects changed contents
on the next authentication attempt and swaps in a complete valid key set; an
invalid replacement fails closed.

The default allowed algorithms are `RS256` and `ES256`. Configure the smallest
set used by the issuer. `HS256` exists for controlled deployments and tests but
is not a suitable default for public OIDC issuers because it uses a shared
secret. The principal identifier is the exact issuer plus token subject, which
prevents equal subjects from different issuers from colliding.

Start API-key and OIDC authentication together, or use either provider alone:

```text
qcli serve \
  --auth-file /etc/qcli/api-keys.toml \
  --oidc-file /etc/qcli/oidc.toml \
  --flight-bind 0.0.0.0:32010 \
  --flight-tls-cert /etc/qcli/tls/server-chain.pem \
  --flight-tls-key /etc/qcli/tls/server-key.pem
```

Clients pass the access token as `Authorization: Bearer <token>` to HTTP or as
Flight SQL bearer metadata. Authentication configuration errors fail closed;
they do not fall through to another provider.

## Mutual TLS

Add `--flight-tls-client-ca /etc/qcli/tls/client-ca.pem` to require a client
certificate signed by that CA. TLS verifies the certificate before the request
reaches qcli. qcli binds the authenticated bearer principal to the SHA-256
fingerprint of the verified leaf certificate. Consequently, a session, query,
result, ticket, or prepared statement created with one certificate cannot be
used with a different certificate, even when both connections present the same
bearer token.

The Flight listener has bounded message size, request timeout, keepalive,
concurrent HTTP/2 streams, connection age, and connection-age grace. Bounded
connection lifetime ensures clients reconnect onto newly deployed certificate
material during rolling replacement. `--flight-trusted-proxy` is an alternative
mode for a gRPC-aware proxy that terminates TLS; it is mutually exclusive with
direct Flight TLS and requires `x-forwarded-proto: https`.

## Identity boundaries

Inbound caller identity and outbound warehouse identity are separate:

- OIDC/API-key/mTLS answers who may call qcli and which qcli targets they may use.
- A target's credential provider answers how qcli authenticates to Trino,
  Databricks SQL, or Snowflake.

M22 does not automatically forward an end-user token, perform OAuth token
exchange, or make every enterprise IdP flow work against every warehouse.
Targets continue to use their configured basic, PAT, bearer, username/password,
or other implemented provider. The credential-provider boundary allows a later
deployment-specific OAuth M2M, token-exchange, workload-identity, or delegated
identity adapter without changing HTTP, Flight SQL, sessions, or authorization.

## Rotation and operations

- JWKS changes reload in-process without restarting listeners.
- API-key file behavior remains the existing startup-loaded policy.
- Server and client-CA certificate changes use rolling qcli instance
  replacement. In-process TLS certificate file watching is not implemented.
- Connection aging is not high availability by itself. Zero-interruption
  instance replacement needs at least two instances behind a draining load
  balancer; shared session/result survival is M23.
- HTTP direct TLS/mTLS is not provided. Deploy HTTP behind the trusted TLS/IdP
  proxy mode; direct TLS and mTLS in M22 apply to Flight SQL.

Audit records use the same principal, target, query, outcome, and ownership
semantics across both protocol frontends. Raw tokens, target secrets, and SQL
remain excluded from default audit output.
