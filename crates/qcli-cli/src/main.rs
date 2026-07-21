use qcli_config::{Config, ConfigError, ResolvedTarget, default_config_path};
use qcli_core::{CoreError, QueryService, SessionManager};
use qcli_driver_api::{EngineAdapter, QueryEvent};
use qcli_driver_demo::DemoAdapter;
use qcli_output::{DisplayOptions, OutputError, OutputFormat, StreamOutput};
use std::env;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug)]
enum AppError {
    Usage(String),
    Config(ConfigError),
    Query(CoreError),
    Input(io::Error),
    Output(OutputError),
}

impl AppError {
    fn exit_code(&self) -> u8 {
        match self {
            Self::Usage(_) | Self::Input(_) => 2,
            Self::Config(_) => 3,
            Self::Query(_) => 5,
            Self::Output(_) => 7,
        }
    }

    fn is_broken_pipe(&self) -> bool {
        matches!(self, Self::Output(error) if error.is_broken_pipe())
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => f.write_str(message),
            Self::Config(error) => error.fmt(f),
            Self::Query(error) => error.fmt(f),
            Self::Input(error) => write!(f, "could not read SQL input: {error}"),
            Self::Output(error) => write!(f, "could not write query results: {error}"),
        }
    }
}

impl From<ConfigError> for AppError {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<CoreError> for AppError {
    fn from(value: CoreError) -> Self {
        Self::Query(value)
    }
}

impl From<OutputError> for AppError {
    fn from(value: OutputError) -> Self {
        Self::Output(value)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(env::args().skip(1).collect()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.is_broken_pipe() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}

async fn run(args: Vec<String>) -> Result<(), AppError> {
    let (config_path, command) = parse_global_args(args)?;
    if command.iter().any(|argument| {
        matches!(
            argument.as_str(),
            "--target" | "--command" | "--file" | "--format"
        )
    }) {
        let query = parse_query_args(&command)?;
        return run_query(&config_path, query).await;
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
            show_config(&Config::load(&config_path)?);
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
        _ => Err(AppError::Usage(
            "unknown command; run qcli without arguments for help".into(),
        )),
    }
}

struct QueryArguments {
    target: String,
    source: QuerySource,
    format: Option<OutputFormat>,
}

enum QuerySource {
    Command(String),
    File(String),
}

fn parse_query_args(arguments: &[String]) -> Result<QueryArguments, AppError> {
    let mut target = None;
    let mut source = None;
    let mut format = None;
    let mut index = 0;
    while index < arguments.len() {
        let flag = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| AppError::Usage(format!("{flag} requires a value")))?;
        match flag.as_str() {
            "--target" if target.is_none() => target = Some(value.clone()),
            "--command" if source.is_none() => source = Some(QuerySource::Command(value.clone())),
            "--file" if source.is_none() => source = Some(QuerySource::File(value.clone())),
            "--format" if format.is_none() => {
                format = Some(
                    OutputFormat::from_str(value)
                        .map_err(|error| AppError::Usage(error.to_string()))?,
                );
            }
            "--command" | "--file" => {
                return Err(AppError::Usage(
                    "specify exactly one of --command or --file".into(),
                ));
            }
            _ => {
                return Err(AppError::Usage(format!(
                    "unknown or repeated query option '{flag}'"
                )));
            }
        }
        index += 2;
    }
    Ok(QueryArguments {
        target: target
            .ok_or_else(|| AppError::Usage("query execution requires --target TARGET".into()))?,
        source: source.ok_or_else(|| {
            AppError::Usage(
                "query execution requires exactly one of --command SQL or --file PATH".into(),
            )
        })?,
        format,
    })
}

async fn run_query(path: &Path, arguments: QueryArguments) -> Result<(), AppError> {
    let sql = match arguments.source {
        QuerySource::Command(sql) => sql,
        QuerySource::File(file) if file == "-" => {
            let mut sql = String::new();
            io::stdin()
                .read_to_string(&mut sql)
                .map_err(AppError::Input)?;
            sql
        }
        QuerySource::File(file) => fs::read_to_string(file).map_err(AppError::Input)?,
    };
    let config = Config::load(path)?;
    let target = config
        .target(&arguments.target)
        .cloned()
        .ok_or_else(|| ConfigError {
            path: path.to_path_buf(),
            line: None,
            message: format!("target '{}' does not exist", arguments.target),
        })?;
    let sessions = SessionManager::default();
    let snapshot = sessions.create(target);
    let decimal_places = option(&snapshot.properties, "decimal_places", 3);
    let string_truncate = option(&snapshot.properties, "string_truncate", 80);
    let format = arguments
        .format
        .or_else(|| {
            snapshot
                .properties
                .get("output_format")
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(OutputFormat::Table);
    let adapters: Vec<Arc<dyn EngineAdapter>> = vec![Arc::new(DemoAdapter)];
    let service = QueryService::new(adapters, 8);
    let mut handle = service.submit(snapshot, sql)?;
    let query_id = handle.id.clone();
    let stdout = io::stdout();
    let mut output = StreamOutput::new(
        io::BufWriter::new(stdout.lock()),
        format,
        DisplayOptions {
            decimal_places,
            string_truncate,
        },
    )?;
    while let Some(batch) = handle.next_batch().await {
        output.write_batch(&batch)?;
    }
    let rendered_rows = output.finish()?;
    let mut engine_query_id = None;
    while let Some(event) = handle.next_event().await {
        if let QueryEvent::EngineQueryId(id) = event {
            engine_query_id = Some(id);
        }
    }
    handle.finish().await?;
    eprintln!("{rendered_rows} rows");
    eprintln!("Query ID: {query_id}");
    if let Some(id) = engine_query_id {
        eprintln!("Engine query ID: {id}");
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

fn parse_global_args(mut args: Vec<String>) -> Result<(PathBuf, Vec<String>), AppError> {
    let mut path = default_config_path()?;
    if args.first().is_some_and(|argument| argument == "--config") {
        if args.len() < 2 {
            return Err(AppError::Usage("--config requires a path".into()));
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
    println!("qcli — one query shell for cloud data platforms\n");
    println!("Usage: qcli [--config PATH] <command>");
    println!(
        "       qcli [--config PATH] --target TARGET (--command SQL | --file PATH) [--format FORMAT]\n"
    );
    println!("Formats: table, vertical, csv, tsv, json, jsonl\n");
    println!("Commands:");
    println!("  config path          Print the configuration path");
    println!("  config check         Validate configuration and targets");
    println!("  config show          Show resolved configuration with secrets redacted");
    println!("  target list          List configured targets");
    println!("  target show NAME     Show one resolved target with secrets redacted");
}
