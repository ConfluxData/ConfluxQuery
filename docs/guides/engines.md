# Configure query engines

## Trino

### Local, unauthenticated coordinator

```ini
[trino-local]
engine = trino
url = http://127.0.0.1:8080
user = local-user
catalog = tpch
schema = tiny
```

```bash
qcli target test trino-local
qcli --target trino-local --command 'select * from tpch.tiny.nation limit 10'
```

### Basic or bearer authentication

```ini
[trino-enterprise]
engine = trino
url = https://trino.example.com
user = ${TRINO_USER}
password = ${TRINO_PASSWORD}
catalog = hive
schema = analytics
```

Use `token=${TRINO_TOKEN}` for bearer auth. ConfluxQuery rejects credentials over plain
HTTP. Enterprise IdP support depends on whether the target accepts a bearer
token ConfluxQuery can obtain/provide; browser-interactive engine login is not implied
by gateway OIDC caller authentication.

Trino `USE catalog.schema` updates tracked context when the server reports the
session change. `\use-catalog` and `\use-schema` provide validated navigation.

## Databricks SQL with PAT

Find the server hostname and HTTP path in the SQL warehouse connection details.

```ini
[databricks-dev]
engine = databricks
auth_type = pat
host = dbc-xxxxxxxx-xxxx.cloud.databricks.com
http_path = /sql/1.0/warehouses/xxxxxxxxxxxxxxxx
token = ${DATABRICKS_TOKEN}
catalog = main
schema = default
```

```bash
export DATABRICKS_TOKEN='dapi...'
qcli target test databricks-dev
qcli --target databricks-dev --command 'select current_catalog(), current_schema()'
```

Use catalog and schema independently:

```sql
USE CATALOG hive_metastore;
USE SCHEMA tpch_1;
```

`USE SCHEMA hive_metastore.tpch_1` can be rejected by Unity Catalog as a nested
namespace. ConfluxQuery preserves this backend error; it does not rewrite namespace
semantics.

## Snowflake username/password

```ini
[snowflake-dev]
engine = snowflake
auth_type = password
account = xy12345.ap-south-1
user = ${SNOWFLAKE_USER}
password = ${SNOWFLAKE_PASSWORD}
warehouse = COMPUTE_WH
database = SNOWFLAKE_SAMPLE_DATA
schema = TPCH_SF1
role = ANALYST
```

```bash
export SNOWFLAKE_USER='ANALYST_USER'
export SNOWFLAKE_PASSWORD='secret-or-supported-token'
qcli target test snowflake-dev
qcli --target snowflake-dev --command \
  'select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION limit 10'
```

Only authentication modes implemented by the ConfluxQuery adapter/client combination
are supported. A value accepted by Snowflake as a password-compatible token can
occupy the `password` field, but ConfluxQuery does not transform a PAT into another
authentication flow. TOTP/MFA and browser SSO require explicit provider/client
support and must not be assumed.

## Validate portability

```bash
qcli target capabilities trino-local
qcli target capabilities databricks-dev
qcli target capabilities snowflake-dev
```

Portable conformance SQL avoids engine-sensitive constructs—for example,
Databricks requires a size for some `VARCHAR` casts. Production SQL remains
native to the selected engine.
