use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::ManagedDriver;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::var("QCLI_FLIGHT_URI").unwrap_or_else(|_| "grpc://127.0.0.1:32010".into());
    let token = std::env::var("QCLI_FLIGHT_TOKEN")?;
    let target = std::env::var("QCLI_FLIGHT_TARGET").unwrap_or_else(|_| "demo".into());
    let query =
        std::env::var("QCLI_FLIGHT_QUERY").unwrap_or_else(|_| "select * from sample".into());
    let expected_rows = std::env::var("QCLI_FLIGHT_EXPECTED_ROWS")
        .unwrap_or_else(|_| "2".into())
        .parse::<usize>()?;
    let mut driver =
        ManagedDriver::load_dynamic_from_filename(DRIVER_PATH, None, AdbcVersion::default())?;
    let database = driver.new_database_with_opts([
        (OptionDatabase::Uri, uri.into()),
        (
            OptionDatabase::Other("adbc.flight.sql.authorization_header".into()),
            format!("Bearer {token}").into(),
        ),
        (
            OptionDatabase::Other("adbc.flight.sql.rpc.call_header.qcli-target".into()),
            target.into(),
        ),
    ])?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(&query)?;
    let mut rows = 0;
    for batch in statement.execute()? {
        rows += batch?.num_rows();
    }
    if rows != expected_rows {
        return Err(format!("expected {expected_rows} rows, got {rows}").into());
    }
    println!("rust-c-driver-manager-adbc: PASS");
    Ok(())
}
