# Optional Cluster Mode and High Availability

qcli remains standalone by default. Cluster mode is explicit and adds a
coordination database plus optional shared result-object storage:

```text
qcli serve

qcli serve \
  --cluster-url "$QCLI_CLUSTER_DATABASE_URL" \
  --node-id qcli-a \
  --result-store-url s3://qcli-results/production \
  --flight-signing-key /run/secrets/qcli-flight-signing-key
```

Do not place database credentials directly in process listings in production;
expand the URL from an appropriately protected secret environment or launcher.

## Separate storage responsibilities

`ClusterStateStore` owns small, mutable, consistency-sensitive state:

- node registration, heartbeat, draining status, and expiry;
- versioned principal-bound resources for sessions and prepared statements;
- fenced query ownership leases;
- distributed expiring quota permits.

`ResultObjectStore` owns large immutable blobs such as retained Arrow results.
It is backed by the Apache Arrow `object_store` abstraction and supports local
file and S3-compatible URLs. Object storage is deliberately not used for node
heartbeats, leases, counters, or compare-and-swap coordination.

PostgreSQL is the first production `ClusterStateStore`. The contract remains
pluggable, but every future implementation must provide atomic conditional
writes, database-authoritative expiry, monotonically increasing fencing tokens,
and principal isolation. Merely implementing SQL syntax is insufficient.

## Presence and failover

Each process registers a stable node ID and renews a 30-second lease every ten
seconds. Presence is informational; it never grants query ownership. A query
owner receives a monotonically increasing fencing token. After a lease expires,
another node may atomically claim a larger token, and any stale owner is unable
to renew or release the new lease.

PostgreSQL uses `clock_timestamp()` for expiry, preventing host clock skew from
changing ownership. Distributed quota permits also expire so a failed process
does not consume capacity forever.

## Shared gateway state

In cluster mode, HTTP and Flight SQL both use shared, principal-bound state for
sessions, session versions, prepared handles, bound Arrow parameters, query
status, distributed quotas, and retained results. A node hydrates shared state
into a local execution cache only after ownership validation. Updates use
compare-and-swap versions, preventing two nodes from silently overwriting one
another.

Completed Arrow results are written to the configured object store before a
remote node advertises them as available. Result keys use a one-way principal
namespace and access still requires the principal-bound PostgreSQL record.

## Flight routing

Every clustered Flight node must use the same 32-byte
`--flight-signing-key`. Session cookies and query/partition tickets are signed,
versioned, expiring, and principal-bound, so any node can verify them without
sticky routing. Rotate this key through a rolling two-key deployment only after
a future multi-key verifier is introduced; M23 supports one active key and a
key change invalidates outstanding tickets.

## Failure and orphan policy

An active owner renews a fenced query lease. Once it expires, exactly one node
can obtain a larger fencing token. Adapters do not yet provide a common safe
reattach contract, so a recovered non-terminal query is marked failed with
`orphaned_query`; qcli never resubmits it automatically and therefore cannot
duplicate a mutation. The warehouse query may continue independently and is
handled according to warehouse-side timeout/governance policy. Completed shared
results remain readable.

During graceful shutdown qcli marks its node `draining` before closing
listeners. Load balancers should remove draining/unready nodes, allow existing
connections to finish, and then stop the process. Schema version validation
fails closed during an incompatible rolling upgrade.

## Limitations

- PostgreSQL is the only production coordination adapter in M23.
- The PostgreSQL client currently uses its configured connection transport;
  deployments must enforce TLS through their trusted network/connection setup.
- Result object URLs support the providers implemented by Arrow `object_store`;
  M23 certifies local storage for tests and S3-compatible storage as the
  production design.
- Active warehouse queries are not reattached after owner loss.
- The shared Flight signing key is single-version rather than a rotation keyring.
