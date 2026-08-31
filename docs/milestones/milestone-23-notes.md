# Milestone 23 Notes: High Availability and Shared State

## Outcome

Milestone 23 keeps standalone qcli unchanged while adding explicit clustered
serve mode. Multiple HTTP and Flight SQL nodes coordinate through PostgreSQL,
share retained Arrow data through an object store, and enforce the same
principal ownership without sticky routing.

## Delivered

- Pluggable `ClusterStateStore` and `ResultObjectStore` boundaries.
- PostgreSQL schema and adapter with database-time expiry.
- Node registration, heartbeat, draining state, and instance versions.
- Monotonic fencing tokens for query ownership and safe expired-lease takeover.
- Principal-bound, versioned session, prepared, and query resources.
- Distributed expiring session/query quota permits.
- Shared session hydration and compare-and-swap mutations.
- Shared prepared SQL and bound Arrow parameter batches.
- Shared query status and immutable Arrow IPC result files.
- Local and S3-compatible object-store adapters through Arrow `object_store`.
- Cross-node HTTP session, query, paging, and result paths.
- Cross-node Flight result streams and a required shared signing key.
- Versioned, expiring, principal-bound node-independent session/ticket tokens.
- Fail-closed cluster schema compatibility checking.
- Graceful draining before listener shutdown.
- Explicit orphan-query policy that never resubmits an unknown mutation.

## Demonstration

The deterministic two-node gateway profile creates a session and query on node
A, observes both from node B, reads node A's Arrow result through node B, binds
a prepared statement on node A, executes it on node B, reads the result through
node A, and proves another principal cannot access any resource.

The Flight profile submits through Flight node A, transfers its signed ticket
to Flight node B, and streams the exact Arrow rows from node B. No sticky
routing or node-local signing key is used.

The PostgreSQL 17 profile validates real migrations, presence, lease expiry,
fenced takeover, stale-owner rejection, distributed quota exclusion, and
release. Deterministic tests additionally cover expiry, draining membership,
object results, and cross-principal isolation.

## Safety decisions

- PostgreSQL coordinates mutable state; object storage never implements locks,
  heartbeats, counters, or leases.
- All expiry comparisons use PostgreSQL time in the production adapter.
- A stale fencing token cannot renew or release a newer owner's lease.
- Shared-resource owner mismatches are returned as not-found at service edges.
- Query mutation is never automatically replayed after owner failure.
- Cluster mode requires explicit PostgreSQL and object-store configuration.
- Clustered Flight additionally requires the same protected 32-byte signing key
  on every node.

## Evidence

```text
cargo test -p qcli-cluster --locked
cargo test -p qcli-service --locked
cargo test -p qcli-flight-sql --locked
cargo test -p qcli-http --locked
cargo test -p qcli --bins --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

The ignored `postgres_coordination_profile` is run in CI or locally with
`QCLI_TEST_POSTGRES_URL`; M23 completion used PostgreSQL 17 in a disposable
container. The established unrelated M6 pseudo-terminal completion timeout
remains outside the changed crates and is not used as M23 evidence.

## Accepted limitations

- Active warehouse queries are marked orphaned rather than reattached.
- The Flight signer has one active key; changing it invalidates outstanding
  cookies and tickets.
- PostgreSQL is the only certified coordination implementation.
- S3-compatible result storage requires provider credentials and lifecycle
  policy supplied by the deployment.
- Cross-region active/active behavior is not certified.

## Next milestone

M24 packages and certifies the unified HTTP, Flight SQL, ADBC, JDBC, and ODBC
connectivity release.
