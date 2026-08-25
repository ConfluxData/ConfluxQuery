# ConfluxQuery Gateway quickstart

ConfluxQuery Gateway is started with `qcli serve`.

## 1. Start a local preview

```bash
qcli --config examples/milestone-2.env serve
```

The preview binds to `127.0.0.1:8088`. Open
`http://127.0.0.1:8088/docs/` for Swagger UI or fetch
`/openapi.json`. Preview mode is for local evaluation; production requires
caller identity.

## 2. Create an API key

```bash
qcli auth key create application-a
```

The raw key is printed once. Store the returned Argon2id hash, never the raw
key, in a mode-`0600` auth file:

```toml
[principals.application-a]
targets = ["demo"]
max_sessions = 5
max_concurrent_queries = 3

[keys.application-a]
principal = "application-a"
secret_hash = "$argon2id$v=19$..."
enabled = true
```

```bash
qcli --config examples/milestone-2.env serve \
  --auth-file ~/.qcli/http-auth.toml
```

## 3. Submit an HTTP query

```bash
export QCLI_TOKEN='qcli_k1_application-a_...'
curl -sS -X POST http://127.0.0.1:8088/v1/queries \
  -H "Authorization: Bearer $QCLI_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"target":"demo","sql":"select * from sample"}'
```

Poll the returned query ID at `/v1/queries/{query_id}` and retrieve rows from
`/v1/queries/{query_id}/results?limit=100`.

## 4. Enable Flight SQL

Flight SQL requires an authenticator:

```bash
qcli --config examples/milestone-2.env serve \
  --auth-file ~/.qcli/http-auth.toml \
  --flight-bind 127.0.0.1:32010
```

Applications can now connect through native Flight SQL, ADBC, or the tested
Arrow Flight SQL JDBC client. See [client examples](../server/clients.md).

## Production boundary

Non-loopback HTTP requires `--trusted-proxy`; the trusted proxy must supply
`x-forwarded-proto: https`. Non-loopback Flight requires direct TLS or
`--flight-trusted-proxy`. Read the [operations runbook](../operations.md)
before exposing either listener.
