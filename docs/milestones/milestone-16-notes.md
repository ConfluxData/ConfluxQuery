# Milestone 16 — Unified Flight sessions

Status: Complete

## Outcome

qcli now implements Apache Arrow Flight's standard `SetSessionOptions`,
`GetSessionOptions`, and `CloseSession` actions. A Flight session is the same
logical, versioned session used by HTTP and the shared gateway service. It is
not a second protocol-specific connection or state store.

Apache Arrow Rust 59.1 does not currently generate the experimental Flight
session messages even though they are part of `Flight.proto`. qcli defines the
missing protobuf messages locally with the specification's package-independent
wire fields and handles them through Arrow Rust's custom-action extension
point. No qcli-specific session RPC was invented.

## Demo flow

1. Authenticate with the normal bearer API key.
2. Call `SetSessionOptions` with at least:

   ```text
   qcli.target = "trino"
   catalog = "hive"
   schema = "analytics"
   ```

3. Preserve the response's `arrow_flight_session_id` cookie.
4. Execute Flight SQL statements normally. `qcli-target` metadata is no longer
   needed while the valid session cookie is supplied.
5. Change options or switch `qcli.target` with another
   `SetSessionOptions`.
6. Inspect effective client-controlled state with `GetSessionOptions`.
7. Explicitly invalidate the session with `CloseSession`.

Stateless M15 clients remain compatible and may continue attaching
`qcli-target` to each statement.

## Standard wire contract

The actions and response messages use Apache Arrow's names and protobuf field
numbers:

- `SetSessionOptions`
- `GetSessionOptions`
- `CloseSession`
- cookie name `arrow_flight_session_id`

The session cookie is returned with `Path=/`, `HttpOnly`, and `SameSite=Strict`.
Direct non-loopback deployments already require TLS; trusted-proxy deployments
must prove forwarded HTTPS under the M14 transport policy.

## Token model

The cookie value is signed with HMAC-SHA-256 and contains only:

- logical session ID;
- session version;
- authenticated principal ID;
- expiry timestamp.

It never contains a database credential, SQL statement, resolved target
properties, physical connection, or engine protocol state. The signing key is
process-local, so tokens intentionally become invalid after restart. M23 owns
distributed signing and shared state.

Every successful session action returns a renewed cookie. Successful
session-based statement submission also renews it. The shared service
independently renews its idle-access timestamp, so both token expiry and server
session TTL must permit access.

## Option mapping

| Flight option | Shared session value |
|---|---|
| `qcli.target` | Named target section |
| `catalog` | Engine catalog override |
| `schema` | Engine schema/database override |
| `timeout` | Query timeout override |
| `engine.<name>` | Adapter property `<name>` |
| `qcli.session_id` | Read-only logical session ID |
| `qcli.version` | Read-only optimistic concurrency version |

String, boolean, integer, finite double, and string-list Flight option values
are accepted and converted to the adapter's string property boundary. A
valueless option removes its override. Unsupported names and invalid values are
reported through the standard per-option error map.

`GetSessionOptions` returns only qcli-owned identifiers and client-controlled
overrides. It never returns the resolved target property map because that map
may contain credentials.

## Mutation and query semantics

The signed cookie carries the version observed by the client. A mutation with a
stale version returns `ABORTED`. Target replacement and all valid option changes
are applied under one shared-core lock and create one new version. Changing the
target clears prior engine overrides before applying the new request's
overrides.

Statement submission captures the current immutable `SessionSnapshot`.
Subsequent target or option changes do not alter an already-submitted query.
The query status records the session ID and exact version used.

Unauthorized target switches return `PERMISSION_DENIED`. A cookie presented by
another principal also returns `PERMISSION_DENIED` before session lookup.
Unknown, closed, or server-expired sessions return `NOT_FOUND`.

## Closure and active queries

`CloseSession` removes the logical session, invalidates subsequent cookie use,
and cooperatively cancels every retained active query belonging to that
session. Query records and already-retained result batches continue to follow
the shared result-retention policy, which keeps audit and cancellation behavior
consistent with HTTP.

## HTTP interoperability

`GetSessionOptions` exposes the non-secret `qcli.session_id`. The same
authenticated principal may use that ID with the versioned HTTP session and
query endpoints. HTTP does not accept the Flight cookie as an authentication
credential: bearer authentication remains mandatory on both protocols. This is
the explicit bridge between the two frontends.

## Verification

Automated coverage demonstrates:

- standard action encoding and response decoding;
- implicit session creation from `qcli.target`;
- target, catalog, and schema mapping;
- session-based query execution without `qcli-target` metadata;
- atomic target-plus-option mutation;
- stale-token/version rejection;
- cross-principal rejection;
- unauthorized target rejection;
- current-option retrieval without resolved secrets;
- explicit closure and use-after-close rejection;
- existing immutable snapshot, ownership, quota, TTL, and cancellation tests.

Run:

```text
cargo test -p qcli-core -p qcli-service
cargo test -p qcli-flight-sql
cargo test --workspace
```

Credential-dependent Trino, Databricks SQL, and Snowflake profiles remain
operator-run tests. Session execution reaches all three through the same
adapter-neutral shared service.

## Next boundary

M17 adds target-aware Flight SQL catalogs, schemas, tables, columns, keys, and
SQL type metadata. M16 intentionally does not implement metadata discovery,
prepared statements, updates, or distributed session storage.
