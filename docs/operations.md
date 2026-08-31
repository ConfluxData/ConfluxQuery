# Operating qcli Serve

This runbook covers installation, deployment, security, scaling, upgrades,
rollback, and incident response for the unified connectivity release.

## Install and verify

Prefer a signed GitHub release archive or OCI archive. Verify the checksum and
Sigstore bundle before extraction:

```text
sha256sum --check SHA256SUMS
cosign verify-blob \
  --bundle qcli-VERSION-x86_64-unknown-linux-gnu.tar.gz.sigstore.json \
  qcli-VERSION-x86_64-unknown-linux-gnu.tar.gz
qcli --version
qcli --config /etc/qcli/targets.env config check
```

Release archives contain the executable, license, changelog, manual page,
shell completions, deployment templates, and the operational documentation.
The OCI image runs as UID/GID 10001 with no writable application filesystem.

## Topologies

Standalone mode has no external state dependency and suits developer or
restart-tolerant deployments. Production clustered mode uses PostgreSQL for
coordination and an S3-compatible object store for retained Arrow results.
See `high-availability.md` for consistency and failure semantics.

The Kubernetes template in `deploy/kubernetes/qcli.yaml` starts two nodes,
uses non-root/read-only security controls, declares resource bounds, supplies a
disruption budget, and separates target/authentication secrets from runtime
coordination secrets. Replace every `OWNER`, `VERSION`, endpoint, and secret
reference before applying it.

Terminate network TLS at a trusted HTTP/2-capable proxy or configure direct
Flight TLS. HTTP proxy mode and Flight proxy mode require the proxy to set
`x-forwarded-proto: https`. Do not expose a plaintext non-loopback listener.

## Readiness and draining

- `GET /health/live` reports that the process can serve HTTP.
- `GET /health/ready` returns 503 after shutdown/draining begins.
- Flight SQL exposes the standard unauthenticated gRPC health service.

On termination, remove readiness, mark the cluster node draining, stop new
work, and allow up to the configured shutdown grace for active queries. The
Kubernetes template provides 45 seconds before forced termination.

## Capacity and scaling

Scale nodes using concurrent query count, HTTP/2 stream saturation, memory,
Arrow result throughput, warehouse latency, and object-store latency—not CPU
alone. Quotas are enforced per principal across nodes. Object lifecycle rules
must retain objects longer than qcli's result TTL and eventually delete expired
objects.

Release gates cover bounded million-row streaming, upload/result limits,
backpressure, cancellation, concurrent HTTP/Flight operation, fenced ownership,
and cross-node result access. They are correctness gates rather than universal
capacity numbers; benchmark with representative schemas, concurrency, regions,
and warehouse latency before setting production limits.

## Upgrade

1. Back up PostgreSQL and verify object-store availability.
2. Read the changelog and compatibility matrix.
3. Run the new image's `config check` against a redacted copy.
4. Deploy one canary with the existing Flight signing key.
5. Confirm readiness, query execution, retained results, and audit delivery.
6. Roll remaining nodes one at a time while the disruption budget holds one
   node available.
7. Retain the prior binary/image until the observation window closes.

Cluster schema validation fails closed when a binary cannot read the existing
state version. Do not rotate the single active Flight signing key during a
normal binary roll because doing so invalidates outstanding cookies/tickets.

## Rollback

Stop the rollout, drain new nodes, and restore the previous artifact with the
same configuration, PostgreSQL endpoint, result store, and signing key. A
rollback is safe only while the prior binary supports the active cluster schema.
If a future release documents an irreversible migration, restore the coordinated
database backup and its matching object-store generation as one recovery unit.

## Incident response

For authentication or suspected data exposure, remove the listener from the
load balancer, preserve audit logs, revoke caller and warehouse credentials,
and rotate affected TLS material. Changing the Flight signing key immediately
revokes all outstanding Flight sessions and tickets.

For an unavailable PostgreSQL store, keep nodes out of readiness rather than
falling back to isolated state. For object-store failure, query execution may
finish but shared retention fails explicitly; do not report a remotely readable
result. For an owner-node loss, completed results remain readable and an
unreattachable active query becomes `orphaned_query` without automatic replay.

Collect the qcli version, node ID, principal-safe audit correlation/query ID,
target name, engine query ID, timestamps, state-store health, and object-store
request status. Never attach raw configuration, bearer tokens, passwords, SQL,
or result data to an incident ticket without an approved secure channel.
