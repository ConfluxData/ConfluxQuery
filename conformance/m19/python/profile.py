#!/usr/bin/env python3
"""qcli M19 Python ADBC Flight SQL compatibility profile."""

import os

import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi


URI = os.environ.get("QCLI_FLIGHT_URI", "grpc://127.0.0.1:32010")
TOKEN = os.environ["QCLI_FLIGHT_TOKEN"]
TARGET = os.environ.get("QCLI_FLIGHT_TARGET", "demo")
QUERY = os.environ.get("QCLI_FLIGHT_QUERY", "select * from sample")
EXPECTED_ROWS = int(os.environ.get("QCLI_FLIGHT_EXPECTED_ROWS", "2"))
TEST_PREPARED = os.environ.get("QCLI_FLIGHT_TEST_PREPARED", "true").lower() == "true"


def connect(token: str = TOKEN):
    prefix = adbc_driver_flightsql.DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
    return adbc_driver_flightsql.dbapi.connect(
        URI,
        db_kwargs={
            adbc_driver_flightsql.DatabaseOptions.AUTHORIZATION_HEADER.value: f"Bearer {token}",
            f"{prefix}qcli-target": TARGET,
            adbc_driver_flightsql.DatabaseOptions.TIMEOUT_QUERY.value: "10",
            adbc_driver_flightsql.DatabaseOptions.TIMEOUT_FETCH.value: "10",
            adbc_driver_flightsql.DatabaseOptions.TIMEOUT_UPDATE.value: "10",
        },
    )


def main() -> None:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(QUERY)
            rows = cursor.fetchall()
            assert len(rows) == EXPECTED_ROWS, rows
            assert cursor.description, "query schema was not exposed"

            if TEST_PREPARED:
                cursor.execute("select ?", ("typed-value",))
                prepared_rows = cursor.fetchall()
                assert prepared_rows == [("typed-value",)], prepared_rows

        objects = connection.adbc_get_objects(depth="tables").read_all()
        assert objects.num_rows > 0, objects

    try:
        with connect("invalid-key") as connection:
            with connection.cursor() as cursor:
                cursor.execute("select 1")
                cursor.fetchall()
        raise AssertionError("invalid bearer token was accepted")
    except Exception as error:
        assert "Unauthenticated" in str(error) or "UNAUTHENTICATED" in str(error), error

    print("python-adbc: PASS")


if __name__ == "__main__":
    main()
