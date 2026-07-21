use qcli_config::{Config, ConfigError, ResolvedTarget, default_config_path};
use qcli_core::{QueryService, SessionManager};
use qcli_driver_api::{EngineAdapter, QueryEvent};
use qcli_driver_demo::DemoAdapter;
use qcli_output::{DisplayOptions, render_table};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

#[tokio::main]
async fn main() -> ExitCode {
    match run(env::args().skip(1).collect()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(3)
        }
    }
}

async fn run(args: Vec<String>) -> Result<(), ConfigError> {
    let (config_path, command) = parse_global_args(args)?;
    if let Some((target_name, sql)) = query_arguments(&command) {
        return run_query(&config_path, target_name, sql).await;
    }
    match command.as_slice() {
        [group, action] if group == "config" && action == "path" => {
            println!("{}", config_path.display());
            Ok(())
        }
        [group, action] if group == "config" && action == "check" => {
            let config = Config::load(&config_path)?;
            println!(
                "Configuration is valid: {} target(s)",
                config.targets().count()
            );
            Ok(())
        }
        [group, action] if group == "config" && action == "show" => {
            let config = Config::load(&config_path)?;
            show_config(&config);
            Ok(())
        }
        [group, action] if group == "target" && action == "list" => {
            let config = Config::load(&config_path)?;
            for target in config.targets() {
                println!("{:<24} {}", target.name, target.engine);
            }
            Ok(())
        }
        [group, action, name] if group == "target" && action == "show" => {
            let config = Config::load(&config_path)?;
            let target = config.target(name).ok_or_else(|| ConfigError {
                path: config_path,
                line: None,
                message: format!("target '{name}' does not exist"),
            })?;
            show_target(target);
            Ok(())
        }
        [] => {
            print_help();
            Ok(())
        }
        _ => Err(ConfigError {
            path: config_path,
            line: None,
            message: "unknown command; run qcli without arguments for help".into(),
        }),
    }
}

fn query_arguments(arguments: &[String]) -> Option<(&str, &str)> {
    if arguments.len() == 4 && arguments[0] == "--target" && arguments[2] == "--command" {
        Some((&arguments[1], &arguments[3]))
    } else {
        None
    }
}

async fn run_query(
    path: &std::path::Path,
    target_name: &str,
    sql: &str,
) -> Result<(), ConfigError> {
    let config = Config::load(path)?;
    let target = config
        .target(target_name)
        .cloned()
        .ok_or_else(|| ConfigError {
            path: path.to_path_buf(),
            line: None,
            message: format!("target '{target_name}' does not exist"),
        })?;
    let sessions = SessionManager::default();
    let snapshot = sessions.create(target);
    let decimal_places = option(&snapshot.properties, "decimal_places", 3);
    let string_truncate = option(&snapshot.properties, "string_truncate", 80);
    let adapters: Vec<Arc<dyn EngineAdapter>> = vec![Arc::new(DemoAdapter)];
    let service = QueryService::new(adapters, 8);
    let mut handle = service
        .submit(snapshot, sql.to_owned())
        .map_err(|error| ConfigError {
            path: path.to_path_buf(),
            line: None,
            message: error.to_string(),
        })?;
    let query_id = handle.id.clone();
    let mut rendered_rows = 0;
    while let Some(batch) = handle.next_batch().await {
        rendered_rows += batch.num_rows();
        let table = render_table(
            &batch,
            DisplayOptions {
                decimal_places,
                string_truncate,
            },
        )
        .map_err(|error| ConfigError {
            path: path.to_path_buf(),
            line: None,
            message: error.to_string(),
        })?;
        print!("{table}");
    }
    let mut engine_query_id = None;
    while let Some(event) = handle.next_event().await {
        if let QueryEvent::EngineQueryId(id) = event {
            engine_query_id = Some(id);
        }
    }
    handle.finish().await.map_err(|error| ConfigError {
        path: path.to_path_buf(),
        line: None,
        message: error.to_string(),
    })?;
    println!("{rendered_rows} rows");
    println!("Query ID: {query_id}");
    if let Some(id) = engine_query_id {
        println!("Engine query ID: {id}");
    }
    Ok(())
}

fn option(
    properties: &std::collections::BTreeMap<String, String>,
    name: &str,
    fallback: usize,
) -> usize {
    properties
        .get(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn parse_global_args(mut args: Vec<String>) -> Result<(PathBuf, Vec<String>), ConfigError> {
    let mut path = default_config_path()?;
    if args.first().is_some_and(|argument| argument == "--config") {
        if args.len() < 2 {
            return Err(ConfigError {
                path,
                line: None,
                message: "--config requires a path".into(),
            });
        }
        path = PathBuf::from(args.remove(1));
        args.remove(0);
    }
    Ok((path, args))
}

fn show_config(config: &Config) {
    println!("path = {}", config.path().display());
    if !config.defaults().is_empty() {
        println!("\n[default]");
        for (name, value) in config.defaults() {
            println!("{name} = {}", value.display_value());
        }
    }
    for target in config.targets() {
        println!("\n[{}]", target.name);
        for (name, value) in &target.properties {
            println!("{name} = {}", value.display_value());
        }
    }
}

fn show_target(target: &ResolvedTarget) {
    println!("target = {}", target.name);
    println!("engine = {}", target.engine);
    for (name, value) in &target.properties {
        if name != "engine" {
            println!("{name} = {}", value.display_value());
        }
    }
}

fn print_help() {
    println!("qcli — one query shell for cloud data platforms");
    println!();
    println!("Usage: qcli [--config PATH] <command>");
    println!("       qcli [--config PATH] --target TARGET --command SQL");
    println!();
    println!("Commands:");
    println!("  config path          Print the configuration path");
    println!("  config check         Validate configuration and targets");
    println!("  config show          Show resolved configuration with secrets redacted");
    println!("  target list          List configured targets");
    println!("  target show NAME     Show one resolved target with secrets redacted");
}
