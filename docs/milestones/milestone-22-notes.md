# Milestone 22 Notes: Enterprise Identity and Transport

## Outcome

Milestone 22 lets the global qcli service accept API keys and enterprise OIDC
access tokens through the same HTTP/Flight authorization model. Flight SQL can
require a CA-verified client certificate and binds every owned resource to the
bearer principal plus that certificate.

## Delivered

- OIDC JWT validation for key ID, algorithm, signature, issuer, audience,
  expiry, and subject.
- Configurable direct group claim with group-to-target and quota policy.
- API-key and OIDC provider composition with configuration failures closed.
- Atomic in-process reload of changed local JWKS contents.
- Direct Flight TLS with optional client-CA verification.
- SHA-256 leaf-certificate binding in the canonical principal identifier.
- Shared HTTP and Flight target, session, query, result, ticket, and prepared
  statement ownership semantics.
- Bounded Flight message size, request timeout, keepalive, concurrent streams,
  connection age, and drain grace.
- Existing direct-TLS versus trusted-gRPC-proxy fail-closed policy.
- An operator guide and example OIDC policy.

## Demonstration

Start qcli with OIDC and mutual TLS:

```text
qcli serve \
  --oidc-file /etc/qcli/oidc.toml \
  --flight-bind 0.0.0.0:32010 \
  --flight-tls-cert /etc/qcli/server-chain.pem \
  --flight-tls-key /etc/qcli/server-key.pem \
  --flight-tls-client-ca /etc/qcli/client-ca.pem
```

An access token in the configured analyst group receives only its mapped
targets and quotas. Replacing the JWKS file with a valid rotated set changes
accepted signing keys without restarting qcli. Reconnecting with a different
valid client certificate creates a different canonical principal, so it cannot
consume the first certificate's resources.

## Security decisions

- Symmetric JWT signing is never selected by default.
- The issuer is part of the principal ID, preventing cross-issuer subject
  collisions.
- TLS performs client-chain verification; qcli uses only a certificate exposed
  by that verified connection.
- An invalid JWKS replacement rejects authentication and does not partially
  replace the last valid snapshot.
- Provider configuration errors do not fall through and accidentally accept a
  credential through another provider.
- Inbound qcli identity is deliberately separate from outbound warehouse
  identity. M22 does not silently forward user tokens to a target.

## Evidence

```text
cargo test -p qcli-auth --locked
cargo test -p qcli-flight-sql --locked
cargo check --workspace --all-targets --locked
```

The authentication suite proves group policy, issuer/audience enforcement,
key rotation, and old-key rejection. The Flight suite proves authentication,
principal-bound tickets/sessions, hardened listener policy, and deterministic
certificate fingerprint mapping. All changed-crate tests pass; Flight tests
require permission to bind ephemeral loopback ports.

## Accepted limitations

- JWKS acquisition is file-based rather than built-in OIDC discovery. A sidecar
  or configuration manager should refresh the file atomically.
- TLS certificate and client-CA rotation uses rolling instance replacement;
  qcli does not watch certificate files in-process.
- Truly interruption-free instance replacement and cross-node resource
  continuity require M23 high availability.
- HTTP uses trusted-proxy TLS for network deployment; direct qcli mTLS is Flight
  SQL only.
- OAuth token exchange, delegated end-user identity, and warehouse-specific IdP
  login remain outbound credential-provider implementations, not automatic
  consequences of enabling qcli OIDC.

## Next milestone

M23 adds shared state and high availability so authenticated sessions, queries,
results, and signed routing survive node loss and rolling upgrades.
