# Milestone 11 Notes: Production HTTP Service

## Outcome

M11 turns the loopback preview into an explicitly secured multi-caller service.
The same `qcli-core` session and query execution path remains shared with the
terminal; production controls live at the HTTP boundary.

## Demo

Copy the public deterministic demo authentication file so its permissions can
be restricted:

```bash
cp examples/milestone-11-auth.toml /tmp/qcli-m11-auth.toml
chmod 600 /tmp/qcli-m11-auth.toml
cargo run -p qcli -- --config examples/milestone-2.env \
  serve --auth-file /tmp/qcli-m11-auth.toml
```

Swagger is available at `http://127.0.0.1:8088/docs/`. Use its Authorize button
with the demo key recorded in the example file, or call the API directly:

```bash
curl -i http://127.0.0.1:8088/v1/sessions

curl -i -H 'Authorization: Bearer qcli_k1_demo-key_694251ef9e5a41a3a3318ae40d7549ea04f86624d9e64b02a3f474bfa0282916' \
  -H 'Content-Type: application/json' \
  -d '{"target":"demo"}' \
  http://127.0.0.1:8088/v1/sessions
```

The first call returns `401`; the second creates a principal-owned session.
The server emits JSON audit records on stderr without credentials or SQL.

## Authentication and authorization

- `Authenticator` is provider-neutral and returns an
  `AuthenticatedPrincipal`; a JWT/OIDC provider can implement the same boundary.
- Opaque API keys use a public key ID plus a 256-bit random secret.
- Only Argon2id hashes are stored. Authentication files require mode `0600` on
  Unix.
- Keys support disabling and RFC 3339 expiry.
- Principals carry target allowlists, session quotas, and concurrent-query
  quotas.
- Sessions, queries, results, SSE events, and cancellation are owner-scoped.
- Cross-principal access returns `404`; disallowed targets return `403`.

Generate independent production key material with:

```bash
qcli auth key create KEY_ID
```

The raw key is printed once. Only the printed hash belongs in the authentication
file.

## Network exposure policy

Loopback remains the safe default. Non-loopback binding is accepted only when
both `--auth-file` and `--trusted-proxy` are present:

```bash
qcli serve \
  --bind 0.0.0.0:8088 \
  --auth-file ~/.qcli/http-auth.toml \
  --trusted-proxy \
  --cors-origin https://query-console.example.com
```

`--trusted-proxy` means qcli is reachable only through an operator-controlled
TLS-terminating reverse proxy. API requests must contain
`X-Forwarded-Proto: https`; otherwise qcli returns `426`. Without trusted-proxy
mode, any `Forwarded` or `X-Forwarded-*` header is rejected. qcli does not trust
forwarded caller identity—the bearer credential remains authoritative.

Direct TLS termination is intentionally not implemented in qcli. The supported
production topology is:

```text
client -- HTTPS --> trusted reverse proxy -- private HTTP --> qcli
```

The operator must firewall the qcli listener so clients cannot bypass the
proxy.

## CORS

CORS is disabled by default. Each `--cors-origin` adds one exact allowed origin;
wildcards and reflected arbitrary origins are not supported. Allowed preflight
requests receive only the qcli methods and headers. Other origins receive
`403`.

## Audit events

Authenticated server mode emits one-line JSON events prefixed by `qcli_audit`.
Events include action, outcome, principal, target, and opaque session/query IDs
where applicable. They intentionally omit:

- bearer keys and engine credentials;
- authorization headers;
- SQL text;
- result values.

Covered actions include authentication, target authorization, session creation
and deletion, query submission and cancellation, and session expiry.

## Expiry and shutdown

- A background task enforces result TTL and session inactivity TTL independently
  of incoming API traffic.
- Session access renews its inactivity lease.
- Expiring a session cancels its active queries, closes it, and records an audit
  event.
- `Ctrl+C` stops accepting new work, marks the service as shutting down, cancels
  active queries, lets HTTP connections drain, and waits up to the configured
  shutdown grace period for query tasks.
- Retained Arrow spill files are deleted when query records expire or are
  dropped.

## Verification

The HTTP test suite demonstrates:

- two-principal authentication and resource isolation;
- target ACLs and per-principal quotas;
- invalid/missing key rejection;
- fail-closed non-loopback binding;
- trusted-proxy HTTPS enforcement;
- rejection of untrusted forwarded headers;
- exact-origin CORS and preflight behavior;
- session and result expiry;
- cancellation and bounded memory/disk retention;
- audit output that omits SQL;
- existing session, query, pagination, SSE, Arrow, and output-parity behavior.

Release gate:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

## Accepted operational limits

- qcli supports TLS termination at a trusted reverse proxy, not direct TLS.
- Authentication configuration is loaded at startup; key rotation requires a
  graceful restart.
- State and spill storage remain process-local; horizontal scaling requires
  sticky routing or a future shared state store.
- Quotas are per process.
