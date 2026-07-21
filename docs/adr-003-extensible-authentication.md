# ADR-003: Extensible Authentication Providers

Status: Accepted

## Context

Databricks and Snowflake deployments use substantially different authentication
methods across developer laptops, automation, CI, and cloud workloads. Query
transport and authentication evolve independently: selecting a query client that
only supports one credential form would make later authentication support an
adapter rewrite.

The first implementation targets are deliberately small:

- Databricks SQL: personal access token (PAT).
- Snowflake: username and password.

These are bootstrap targets, not the boundary of the design.

## Decision

qcli separates credential acquisition from engine query transport. Each engine
adapter consumes an engine-specific authenticated request or connection produced
by an authentication provider. The core session and query contracts never carry
raw passwords, private keys, refresh tokens, or authorization headers.

The internal model has four responsibilities:

1. `AuthConfig` selects an authentication method and contains redacted or
   non-secret configuration.
2. `CredentialProvider` acquires credentials and renews them when applicable.
3. The engine adapter applies credentials to a request or connection without
   logging them.
4. A secret resolver obtains referenced values from environment variables,
   files, OS secure storage, or future external providers.

Authentication is capability-driven. A driver reports supported authentication
methods, whether interaction is required, and whether credentials can be renewed.
Unsupported methods fail during target validation, before query submission.

The initial configuration is explicit:

```ini
[databricks-dev]
engine = databricks
auth_type = pat
host = https://dbc-example.cloud.databricks.com
warehouse_id = abc123
token = ${DATABRICKS_TOKEN}

[snowflake-dev]
engine = snowflake
auth_type = password
account = organization-account
user = deepak
password = ${SNOWFLAKE_PASSWORD}
```

If `auth_type` is omitted, an adapter may infer it only when the supplied fields
identify exactly one method. Ambiguous combinations are configuration errors.

## Planned authentication matrix

| Engine | Initial | Planned extensions |
|---|---|---|
| Databricks | PAT | OAuth M2M, OAuth U2M/browser, existing Databricks profile/CLI credentials, supplied OAuth token, OIDC/workload identity |
| Snowflake | Username/password | Key-pair JWT, OAuth token and refresh flow, external browser/SSO, programmatic access token, existing Snowflake profile, workload identity federation |
| Trino | Basic, bearer token | OAuth/OIDC, Kerberos, client certificate, deployment-specific providers |

Adding a method must not change the query lifecycle, result model, terminal
frontend, or HTTP API contracts.

## Credential lifecycle

- Providers may return expiring credentials and an expiry time.
- Refresh is synchronized so concurrent queries do not cause a refresh storm.
- A query may retry authentication only when the operation is safe to retry.
- Interactive providers are allowed in the terminal frontend but must never
  unexpectedly open a browser in batch or HTTP-server mode.
- HTTP sessions reference an authorized target credential provider; credentials
  are never copied into HTTP session state or returned by an endpoint.
- Logout or provider invalidation removes cached credentials where supported.

## Security invariants

- Secrets use redacted wrapper types and are not serializable for diagnostics.
- `~/.qcli/.env` may contain secret references; OS secure storage is preferred
  for refresh tokens and other reusable interactive credentials.
- Private keys are referenced by path rather than embedded by default.
- Authorization headers, passwords, tokens, private-key material, and refresh
  tokens never appear in logs, history, errors, metrics, or HTTP responses.
- Authentication configuration participates in target validation but secret
  values do not participate in debug output or cache keys.
- Metadata and result caches remain isolated by effective identity and role.

## Client selection consequence

Authentication breadth is a release gate and a high-weight client-selection
criterion. A community Rust client is acceptable. It must be replaceable behind
the adapter and credential-provider boundaries. When a client couples query
transport to insufficient authentication, qcli may use its protocol/result layer
while supplying authentication itself, or use the vendor REST API directly.

Every Databricks and Snowflake client spike records:

- supported authentication methods and missing methods;
- ability to inject or refresh credentials;
- query submission, polling, pagination/streaming, cancellation, metadata, and
  type fidelity;
- TLS, proxy, timeout, retry, and dependency behavior;
- maintenance activity, license, and a fallback plan.

## Consequences

- PAT and password provide small, demonstrable first integrations.
- Later enterprise authentication methods do not require redesigning qcli core.
- Configuration validation and testing are slightly more involved.
- A candidate with excellent query coverage can still be rejected if its
  authentication layer cannot be extended or replaced.
