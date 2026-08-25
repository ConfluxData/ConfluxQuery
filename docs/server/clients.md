# ConfluxQuery Gateway client ecosystem

The examples assume a gateway at `127.0.0.1:32010`, a bearer token in
`QCLI_FLIGHT_TOKEN`, and target `demo`. Replace `grpc://` with the TLS URI form
required by the chosen client in production.

## Python ADBC

```python
import os
import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi

prefix = adbc_driver_flightsql.DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
with adbc_driver_flightsql.dbapi.connect(
    "grpc://127.0.0.1:32010",
    db_kwargs={
        adbc_driver_flightsql.DatabaseOptions.AUTHORIZATION_HEADER.value:
            f"Bearer {os.environ['QCLI_FLIGHT_TOKEN']}",
        f"{prefix}qcli-target": "demo",
        adbc_driver_flightsql.DatabaseOptions.TIMEOUT_QUERY.value: "30",
    },
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("select * from sample")
        print(cursor.fetchall())
```

Prepared values use normal DB-API parameters: `cursor.execute("select ?",
("value",))`. Metadata is available through `connection.adbc_get_objects()`.

## Go ADBC

```go
driver := flightsql.NewDriver(memory.DefaultAllocator)
database, err := driver.NewDatabase(map[string]string{
    adbc.OptionKeyURI: "grpc://127.0.0.1:32010",
    flightsql.OptionAuthorizationHeader: "Bearer " + os.Getenv("QCLI_FLIGHT_TOKEN"),
    flightsql.OptionRPCCallHeaderPrefix + "qcli-target": "demo",
})
if err != nil { log.Fatal(err) }
connection, err := database.Open(context.Background())
if err != nil { log.Fatal(err) }
defer connection.Close()
statement, _ := connection.NewStatement()
defer statement.Close()
_ = statement.SetSqlQuery("select * from sample")
reader, _, err := statement.ExecuteQuery(context.Background())
if err != nil { log.Fatal(err) }
defer reader.Release()
for reader.Next() { fmt.Println(reader.RecordBatch()) }
```

The repository conformance profile also checks `GetObjects` metadata.

## Java JDBC (upstream Arrow driver)

!!! warning
    This is the upstream Arrow Flight SQL JDBC integration, not the planned
    branded ConfluxQuery M25 driver.

```java
String url = "jdbc:arrow-flight-sql://127.0.0.1:32010/?useEncryption=false";
Properties properties = new Properties();
properties.setProperty("token", System.getenv("QCLI_FLIGHT_TOKEN"));
properties.setProperty("qcli-target", "demo");
properties.setProperty("connectTimeout", "10000");

try (Connection connection = DriverManager.getConnection(url, properties);
     PreparedStatement statement = connection.prepareStatement("select ?")) {
  statement.setString(1, "typed-value");
  try (ResultSet rows = statement.executeQuery()) {
    while (rows.next()) System.out.println(rows.getString(1));
  }
}
```

`DatabaseMetaData` catalogs/tables and `Statement.cancel()` are covered by the
release profile.

## Java ADBC

Use Apache Arrow ADBC's Flight SQL driver, set the URI, authorization header,
and `qcli-target` RPC header, then consume Arrow vectors or JDBC-style adapters.
The exact Maven dependencies and executable profile live under
`conformance/m19/java` and are pinned by release CI.

## Rust

Rust applications can use Arrow Flight's generated Flight SQL client for
native Arrow access. For the HTTP control plane, a minimal `reqwest` flow is:

```rust
let client = reqwest::Client::new();
let query: serde_json::Value = client
    .post("http://127.0.0.1:8088/v1/queries")
    .bearer_auth(std::env::var("QCLI_TOKEN")?)
    .json(&serde_json::json!({"target": "demo", "sql": "select * from sample"}))
    .send().await?.error_for_status()?.json().await?;
println!("query id: {}", query["id"]);
```

The repository's Rust ADBC profile exercises the ADBC C driver manager against
ConfluxQuery Gateway Flight SQL.

## JavaScript / TypeScript HTTP

```javascript
const response = await fetch(`${process.env.QCLI_URL}/v1/queries`, {
  method: "POST",
  headers: {
    Authorization: `Bearer ${process.env.QCLI_TOKEN}`,
    "Content-Type": "application/json",
  },
  body: JSON.stringify({ target: "demo", sql: "select * from sample" }),
});
if (!response.ok) throw new Error(await response.text());
const query = await response.json();
console.log(query.id);
```

Browser applications additionally require their exact origin in
`--cors-origin`.

## Python HTTP

```python
import os, requests
headers = {"Authorization": f"Bearer {os.environ['QCLI_TOKEN']}"}
query = requests.post(
    f"{os.environ['QCLI_URL']}/v1/queries",
    headers=headers,
    json={"target": "demo", "sql": "select * from sample"},
    timeout=10,
).json()
rows = requests.get(
    f"{os.environ['QCLI_URL']}/v1/queries/{query['id']}/results?limit=100",
    headers=headers,
    timeout=10,
).json()
```

## C, C++, and R

Use a compatible ADBC Flight SQL driver and driver manager. These ecosystems
are protocol-reachable, but only named clients/versions in the compatibility
matrix are release-supported. Treat unlisted bindings as an integration to
qualify, not an automatic certification.

## ODBC and BI tools

ODBC remains experimental. There is no approved M24 ODBC/BI workflow. Keep it
out of production support commitments until M20 names and passes exact driver
and BI client combinations.
