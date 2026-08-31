# M20 ODBC certification profile

This profile validates a registered Flight SQL ODBC driver against `qcli
serve`. It is deliberately independent of a particular ODBC driver manager and
can run through Windows ODBC, unixODBC, or a supported macOS manager.

The approved long-term driver is the Apache Arrow Flight SQL ODBC driver. Arrow
25.0.0 contains the Apache-2.0 implementation but does not publish end-user
packages. Until Apache publishes supported artifacts, qcli treats source-built
or nightly driver results as `Experimental`, not `Supported`.

Install and register the driver, start qcli's demo gateway as described in
`docs/odbc-bi-connectivity.md`, then run:

```text
python -m pip install -r conformance/m20/python/requirements.txt
QCLI_ODBC_TOKEN=<key> python conformance/m20/python/profile.py
```

The default DSN-less connection uses:

```text
DRIVER={Apache Arrow Flight SQL ODBC Driver};
HOST=127.0.0.1;
PORT=32010;
TOKEN=<key>;
qcli-target=demo;
useEncryption=false;
useWideChar=true;
```

The profile covers authentication rejection, target propagation, execution,
schema and row retrieval, `SQLTables`, `SQLColumns`, `SQLGetTypeInfo`, SQL info,
diagnostics, and cancellation. Set `QCLI_ODBC_REQUIRE_CANCEL=true` only for a
driver version that implements `SQLCancel`; the current upstream Apache driver
explicitly reports that operation as unsupported.

For protected engine certification, set `QCLI_ODBC_QUERY`,
`QCLI_ODBC_EXPECTED_ROWS`, and `QCLI_ODBC_TARGET`. A complete connection string
may instead be supplied through `QCLI_ODBC_CONNECTION_STRING`; in that mode the
invalid-token check is skipped because the profile cannot safely rewrite an
opaque connection string.
