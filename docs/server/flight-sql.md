# Arrow Flight SQL

Flight SQL is qcli's high-performance, language-neutral application data
plane. It carries Arrow schemas and record batches without JSON conversion and
allows standard ADBC and JDBC clients to reuse the same gateway.

## Connect and select a target

Authenticate with a bearer token. Stateless clients attach `qcli-target` RPC
metadata. Session-aware clients call `SetSessionOptions` with `qcli.target` and
then retain the `arrow_flight_session_id` cookie.

## Implemented protocol areas

- Handshake/authentication and SQL info discovery.
- Statement query/update execution and signed result tickets.
- Standard session create, get, set, target/context mutation, and close.
- Catalog, schema, table, table-type, primary-key, and SQL metadata using exact
  Flight SQL schemas.
- Prepared statement create, typed Arrow parameter bind, execute, and close.
- Query cancellation.
- Arrow `DoPut` ingestion with bounded create/append/replace semantics.
- Multi-endpoint large result reads.
- Health service and transport/message/stream limits.

## Tickets and ownership

Tickets are opaque, signed, expiring, replayable during result retention, and
bound to the authenticated principal. Session cookies and prepared handles
have equivalent ownership/version guarantees. In cluster mode every node uses
the same protected signing key and shared result store.

## TLS

Direct TLS:

```bash
qcli serve --auth-file auth.toml \
  --flight-bind 0.0.0.0:32010 \
  --flight-tls-cert server-chain.pem \
  --flight-tls-key server-key.pem
```

Add `--flight-tls-client-ca client-ca.pem` for mTLS. Alternatively use
`--flight-trusted-proxy` behind a gRPC-aware TLS proxy that forwards HTTPS
transport metadata. Do not use a generic HTTP/1 proxy.

## Metadata and backend differences

Flight SQL supplies a uniform metadata wire schema, but catalog hierarchy is
engine-specific. Trino commonly uses catalog/schema; Databricks uses Unity
Catalog catalog/schema; Snowflake uses database/schema. qcli maps these through
the adapter and preserves native names/types where possible.

## Ingestion

Ingestion is bounded, atomic on failure, and idempotent for a retried request
identifier. The selected engine/target must support the requested table
operation. See [ingestion and transfer](../ingestion-and-advanced-transfer.md).
