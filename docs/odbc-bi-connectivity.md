# ODBC and BI connectivity

## Current status

ConfluxQuery's ODBC path is **Experimental**. It is not yet a supported product
claim.
The server exposes the standard Flight SQL operations needed by ODBC clients,
and `conformance/m20` provides a reusable certification profile, but the
selected upstream driver still has release and cancellation gaps.

| Component | Selection | License | Status |
|---|---|---|---|
| Flight SQL ODBC driver | Apache Arrow 25 source | Apache-2.0 | Experimental; no official end-user package |
| Windows manager | Windows 64-bit ODBC Driver Manager | Operating-system component | Profile ready; execution evidence pending |
| Linux manager | unixODBC | LGPL-2.1-or-later | Profile ready; execution evidence pending |
| macOS manager | Driver-compatible iODBC/unixODBC | Manager-specific | Profile ready; execution evidence pending |
| BI application | Power BI or Excel on Windows; Excel on supported macOS | Proprietary | Manual certification pending |

Apache Arrow 23 completed the Flight SQL ODBC implementation, and Arrow 25
still states that packages are not distributed to end users. Apache CI
artifacts and source builds are suitable for development evidence only. qcli
will not download an unversioned nightly or redistribute a vendor binary as a
supported dependency.

Dremio's published Arrow Flight SQL ODBC driver is a useful comparison path,
but is LGPL-2 licensed, distributed under Dremio's packaging policy, and its
documented macOS support is Intel-only. It is not qcli's default dependency.

## Connection configuration

The Apache driver passes unrecognized connection properties as Flight RPC
headers. This lets qcli receive the `qcli-target` header without a custom ODBC
fork.

DSN-less development connection:

```text
DRIVER={Apache Arrow Flight SQL ODBC Driver};
HOST=127.0.0.1;
PORT=32010;
TOKEN=<raw-qcli-api-key>;
qcli-target=demo;
useEncryption=false;
useWideChar=true;
```

Production connections must use `useEncryption=true`, retain certificate
verification, and configure either the system trust store or `trustedCerts`.
Do not save qcli tokens in a repository-owned DSN.

Equivalent `odbc.ini` entry:

```ini
[qcli-demo]
Driver = Apache Arrow Flight SQL ODBC Driver
Host = gateway.example.com
Port = 32010
Token = <raw-qcli-api-key>
qcli-target = demo
useEncryption = true
useWideChar = true
```

## Certification profile

Start the deterministic gateway:

```text
qcli --config examples/milestone-2.env serve \
  --bind 127.0.0.1:18089 \
  --auth-file examples/milestone-11-auth.toml \
  --flight-bind 127.0.0.1:32010
```

Run `conformance/m20/python/profile.py` with pyodbc 5.3.0. The profile checks:

- bearer rejection and `qcli-target` routing;
- execution, row retrieval, schema, null/type conversion;
- table, column, type, DBMS name, and version metadata;
- surfaced ODBC diagnostic records;
- cancellation when required by the selected driver profile.

## BI certification procedure

A BI combination becomes supported only after evidence records all of these:

1. Install the exact signed driver artifact and record its checksum.
2. Create a user or system DSN without embedding credentials in screenshots or
   logs.
3. Connect using a restricted qcli principal and select an allowed target.
4. Browse catalogs, schemas, tables, and columns.
5. Import a dataset containing integers, Unicode strings, decimals, nulls,
   dates, timestamps, and binary values.
6. Refresh the dataset and verify row counts and values.
7. Exercise query timeout and cancellation.
8. Confirm invalid credentials, unauthorized targets, and invalid SQL expose
   actionable diagnostics without secrets.
9. Record application, driver, OS, qcli, and engine versions in
   `docs/milestones/milestone-20-notes.md`.

## Known blockers

- Apache Arrow 25 does not publish supported ODBC packages.
- The current Apache driver implements `SQLPrepare` but not parameter binding,
  and its `SQLCancel` entry point reports `HYC00`/not implemented.
- The upstream driver documents a Linux metadata limitation for interactive
  `isql` `tables` and `columns` commands.
- No qcli Power BI or Excel run has yet produced repository-recorded evidence.

M20 can move to `Complete` only after a pinned driver artifact passes the
supported-platform profile, cancellation works, and at least one representative
BI discovery/query workflow has recorded evidence.
