#!/usr/bin/env python3
"""qcli M20 ODBC compatibility profile.

The profile is driver-manager neutral. Install and register the selected ODBC
driver first, then provide either QCLI_ODBC_CONNECTION_STRING or the individual
QCLI_ODBC_* values documented below.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any

import pyodbc


DRIVER = os.environ.get(
    "QCLI_ODBC_DRIVER", "Apache Arrow Flight SQL ODBC Driver"
)
HOST = os.environ.get("QCLI_ODBC_HOST", "127.0.0.1")
PORT = os.environ.get("QCLI_ODBC_PORT", "32010")
TOKEN = os.environ.get("QCLI_ODBC_TOKEN", "")
TARGET = os.environ.get("QCLI_ODBC_TARGET", "demo")
QUERY = os.environ.get("QCLI_ODBC_QUERY", "select * from sample")
EXPECTED_ROWS = int(os.environ.get("QCLI_ODBC_EXPECTED_ROWS", "2"))
REQUIRE_CANCEL = os.environ.get("QCLI_ODBC_REQUIRE_CANCEL", "false").lower() == "true"


def connection_string(token: str = TOKEN) -> str:
    explicit = os.environ.get("QCLI_ODBC_CONNECTION_STRING")
    if explicit:
        return explicit
    if not token:
        raise RuntimeError(
            "QCLI_ODBC_TOKEN or QCLI_ODBC_CONNECTION_STRING must be provided"
        )
    return ";".join(
        [
            f"DRIVER={{{DRIVER}}}",
            f"HOST={HOST}",
            f"PORT={PORT}",
            f"TOKEN={token}",
            f"qcli-target={TARGET}",
            "useEncryption=false",
            "useWideChar=true",
            "",
        ]
    )


def rows(cursor: pyodbc.Cursor) -> list[Any]:
    return list(cursor.fetchall())


def check_query(connection: pyodbc.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(QUERY)
    result = rows(cursor)
    assert len(result) == EXPECTED_ROWS, result
    assert cursor.description, "ODBC did not expose the result schema"


def check_metadata(connection: pyodbc.Connection) -> None:
    cursor = connection.cursor()
    catalogs = rows(cursor.tables())
    assert catalogs, "SQLTables returned no objects"

    columns = rows(cursor.columns())
    assert columns, "SQLColumns returned no columns"

    type_info = rows(cursor.getTypeInfo())
    assert type_info, "SQLGetTypeInfo returned no types"

    dbms_name = connection.getinfo(pyodbc.SQL_DBMS_NAME)
    dbms_version = connection.getinfo(pyodbc.SQL_DBMS_VER)
    assert dbms_name and dbms_version, (dbms_name, dbms_version)


def check_diagnostics(connection: pyodbc.Connection) -> None:
    try:
        connection.cursor().execute("fail").fetchall()
    except pyodbc.Error as error:
        assert error.args, "ODBC error contained no diagnostic record"
        sql_state = str(error.args[0])
        assert len(sql_state) == 5, error.args
    else:
        raise AssertionError("failing query unexpectedly succeeded")


def check_rejected_token() -> None:
    if os.environ.get("QCLI_ODBC_CONNECTION_STRING"):
        return
    try:
        pyodbc.connect(connection_string("invalid-key"), timeout=5)
    except pyodbc.Error:
        return
    raise AssertionError("invalid bearer token was accepted")


def check_cancel(connection: pyodbc.Connection) -> None:
    cursor = connection.cursor()
    outcome: list[BaseException | None] = []

    def execute() -> None:
        try:
            cursor.execute("wait-for-cancel").fetchall()
            outcome.append(None)
        except BaseException as error:  # the driver determines the exception type
            outcome.append(error)

    worker = threading.Thread(target=execute, daemon=True)
    worker.start()
    time.sleep(0.2)
    try:
        cursor.cancel()
    except pyodbc.Error as error:
        if REQUIRE_CANCEL:
            raise AssertionError("ODBC cancellation is required but unsupported") from error
        print(f"odbc-cancel: UNSUPPORTED ({error.args[0] if error.args else 'unknown'})")
        worker.join(timeout=6)
        return

    worker.join(timeout=3)
    assert not worker.is_alive(), "cancel did not terminate the ODBC operation"
    assert outcome and outcome[0] is not None, "cancelled query completed successfully"


def main() -> None:
    with pyodbc.connect(connection_string(), timeout=10, autocommit=True) as connection:
        check_query(connection)
        check_metadata(connection)
        check_diagnostics(connection)
        check_cancel(connection)
    check_rejected_token()
    print("odbc-profile: PASS")


if __name__ == "__main__":
    main()
