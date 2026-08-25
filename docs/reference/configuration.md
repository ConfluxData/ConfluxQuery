# Configuration property reference

Properties are case-sensitive lowercase names. Unknown values fail validation.
Target values override `[default]` values.

## Portable properties

| Property | Type | Purpose |
|---|---|---|
| `engine` | enum | Target adapter: `trino`, `databricks`, `snowflake`, `demo`. |
| `output_format` | string | `table`, `vertical`, `csv`, `tsv`, `json`, `jsonl`. |
| `decimal_places` | integer | Human-display decimal precision. |
| `decimal_rounding` | string | Decimal display rounding policy. |
| `strip_trailing_decimal_zeros` | boolean | Remove insignificant display zeros. |
| `string_truncate` | integer | Human-display string limit. |
| `binary_format` | string | Binary display convention. |
| `null_value` | string | Human rendering for null. |
| `table_style` | string | Human table border style. |
| `color` | string | Color policy. |
| `expanded` | string | Expanded/vertical display preference. |
| `headers` | boolean | Emit column headers. |
| `row_numbers` | boolean | Show human row numbers. |
| `max_column_width` | integer | Human table column bound. |
| `timestamp_format` | string | Human timestamp formatting. |
| `timezone` | string | Display/session timezone preference. |
| `timing` | boolean | Print elapsed query time. |
| `query_timeout` | duration | Query timeout such as `30s`, `5m`, `1h`. |
| `connect_timeout` | duration | Connection timeout. |
| `fetch_size` | integer | Preferred adapter fetch batch. |
| `page_size` | integer | Preferred result page size. |
| `max_display_rows` | integer | Human display safety bound. |
| `progress` | string | Progress rendering policy. |
| `retry` | string | Retry policy preference. |
| `history` | boolean | Enable safe interactive history. |
| `history_limit` | integer | Maximum retained history entries. |
| `syntax_highlight` | boolean | Interactive highlighting. |
| `completion` | string | Completion policy. |
| `pager` | string | Pager behavior/program. |
| `editor` | string | External editor preference. |
| `prompt` | string | Prompt template/preference. |
| `confirm_target_switch` | boolean | Target-switch confirmation policy. |
| `tls_verify` | boolean | Verify backend TLS certificates. |
| `show_query_id` | boolean | Print qcli/native query IDs. |
| `log_level` | string | Runtime logging preference. |

Boolean values are exactly `true` or `false`; integer values are non-negative.
Durations use `ms`, `s`, `m`, or `h` suffixes.

## Trino

| Property | Purpose |
|---|---|
| `url` | Coordinator base URL. |
| `user` | Trino user identity. |
| `password` | Basic-auth password; secret. |
| `token` | Bearer token; secret. |
| `catalog`, `schema` | Initial context. |
| `source` | Trino client source. |
| `client_tags` | Client tag set. |

Credentials over plain HTTP are rejected. Local unauthenticated development
may use HTTP without password/token.

## Databricks SQL

| Property | Purpose |
|---|---|
| `auth_type` | Currently released `pat`; provider boundary is extensible. |
| `host` | Workspace hostname. |
| `http_path` | SQL warehouse HTTP path. |
| `token` | Personal access token; secret. |
| `catalog`, `schema` | Initial Unity Catalog context. |
| `user` | Optional user identity metadata. |

## Snowflake

| Property | Purpose |
|---|---|
| `auth_type` | Released password flow; extensible provider selector. |
| `account` | Snowflake account identifier. |
| `user`, `password` | Username and secret/password-compatible credential. |
| `private_key` | Secret key material/path for future/provider use. |
| `warehouse` | Compute warehouse. |
| `database`, `schema`, `role` | Initial context and role. |

Authentication support depends on the selected Rust client and qcli adapter.
Do not assume every Snowflake authentication method is implemented merely
because Snowflake supports it.

## Server environment variables

| Variable | Equivalent |
|---|---|
| `QCLI_CLUSTER_URL` | `--cluster-url` |
| `QCLI_NODE_ID` | `--node-id` |
| `QCLI_RESULT_STORE_URL` | `--result-store-url` |
| `QCLI_FLIGHT_SIGNING_KEY` | `--flight-signing-key` path |

Engine secrets may use arbitrary environment names referenced as `${NAME}`.
