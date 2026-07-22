use qcli_config::{Config, ResolvedTarget, default_config_path};
use qcli_driver_api::{AdapterCapability, EngineAdapter};
use qcli_driver_conformance::{assert_common_capabilities, assert_portable_query, run_query};
use qcli_driver_databricks::DatabricksAdapter;
use qcli_driver_snowflake::SnowflakeAdapter;
use qcli_driver_trino::TrinoAdapter;
use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

const VALIDATION_SQL: &str = include_str!("../../../examples/validation.sql");
type LiveTarget = (
    &'static str,
    Arc<dyn EngineAdapter>,
    BTreeMap<String, String>,
);

#[test]
fn release_candidate_adapters_expose_the_common_minimum() {
    let adapters: Vec<Arc<dyn EngineAdapter>> = vec![
        Arc::new(TrinoAdapter),
        Arc::new(DatabricksAdapter),
        Arc::new(SnowflakeAdapter),
    ];
    for adapter in &adapters {
        assert_common_capabilities(adapter.as_ref());
    }

    assert!(
        adapters[0]
            .capabilities()
            .supports(AdapterCapability::CancelQuery)
    );
    assert!(
        adapters[1]
            .capabilities()
            .supports(AdapterCapability::CancelQuery)
    );
    assert!(
        !adapters[2]
            .capabilities()
            .supports(AdapterCapability::CancelQuery),
        "Snowflake cancellation must remain explicitly unsupported until query IDs are available"
    );
}

#[tokio::test]
#[ignore = "requires configured live Trino, Databricks, and Snowflake targets"]
async fn live_three_engine_portable_query_profile() {
    let config_path =
        env::var_os("QCLI_M9_CONFIG").map_or_else(|| default_config_path().unwrap(), PathBuf::from);
    let config = Config::load(&config_path).unwrap();
    let targets: Vec<LiveTarget> = vec![
        (
            "trino",
            Arc::new(TrinoAdapter),
            target_properties(&config, "QCLI_M9_TRINO_TARGET", "trino", "trino"),
        ),
        (
            "databricks",
            Arc::new(DatabricksAdapter),
            target_properties(
                &config,
                "QCLI_M9_DATABRICKS_TARGET",
                "databricks-dev",
                "databricks",
            ),
        ),
        (
            "snowflake",
            Arc::new(SnowflakeAdapter),
            target_properties(
                &config,
                "QCLI_M9_SNOWFLAKE_TARGET",
                "snowflake-dev",
                "snowflake",
            ),
        ),
    ];

    for (target, adapter, properties) in targets {
        let outcome = run_query(adapter, target, properties, VALIDATION_SQL)
            .await
            .unwrap();
        assert_portable_query(&outcome);
        if target != "snowflake" {
            assert!(
                outcome.engine_query_id().is_some(),
                "{target} did not expose an engine query ID"
            );
        }
    }
}

fn target_properties(
    config: &Config,
    variable: &str,
    fallback: &str,
    expected_engine: &str,
) -> BTreeMap<String, String> {
    let name = env::var(variable).unwrap_or_else(|_| fallback.into());
    let target: &ResolvedTarget = config
        .target(&name)
        .unwrap_or_else(|| panic!("target '{name}' selected by {variable} does not exist"));
    assert_eq!(
        target.engine, expected_engine,
        "target '{name}' has wrong engine"
    );
    target
        .properties
        .iter()
        .map(|(name, value)| (name.clone(), value.expose().to_owned()))
        .collect()
}
