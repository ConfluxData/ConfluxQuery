use qcli_config::{Config, ConfigError, ResolvedTarget, default_config_path};
use qcli_core::{CoreError, QueryService, SessionManager};
use qcli_driver_api::{AdapterCapability, EngineAdapter, QueryEvent};
use qcli_driver_databricks::DatabricksAdapter;
use qcli_driver_demo::DemoAdapter;
use qcli_driver_snowflake::SnowflakeAdapter;
use qcli_driver_trino::TrinoAdapter;
use qcli_http::{HttpLimits, HttpService, bind_local};
use qcli_output::{DisplayOptions, OutputError, OutputFormat, StreamOutput};
use qcli_repl::ReplError;
use std::env;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
enum AppError {
    Usage(String),
    Config(ConfigError),
    Query(CoreError),
    Input(io::Error),
    Output(OutputError),
    Repl(ReplError),
    Server(io::Error),
}

impl AppError {
    fn exit_code(&self) -> u8 {
        match self {
            Self::Usage(_) | Self::Input(_) => 2,
            Self::Config(_) => 3,
            Self::Query(CoreError::Driver(error))
                if matches!(
                    error.code.as_str(),
                    "authentication" | "connection" | "insecure_authentication" | "timeout"
                ) =>
            {
                4
            }
            Self::Query(_) => 5,
            Self::Output(_) => 7,
            Self::Repl(_) => 6,
            Self::Server(_) => 8,
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
            Self::Repl(error) => write!(f, "interactive terminal failed: {error}"),
            Self::Server(error) => write!(f, "HTTP service failed: {error}"),
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

impl From<ReplError> for AppError {
    fn from(value: ReplError) -> Self {
        Self::Repl(value)
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
    if command
        .iter()
        .any(|argument| matches!(argument.as_str(), "--command" | "--file" | "--format"))
    {
        let query = parse_query_args(&command)?;
        return run_query(&config_path, query).await;
    }
    if command.is_empty()
        || command
            .first()
            .is_some_and(|argument| argument == "--target")
    {
        let target = match command.as_slice() {
            [] => None,
            [flag, target] if flag == "--target" => Some(target.as_str()),
            _ => {
                return Err(AppError::Usage(
                    "interactive usage: qcli [--target TARGET]".into(),
                ));
            }
        };
        let config = Config::load(&config_path)?;
        let history = qcli_repl::history_path(&config_path);
        return qcli_repl::run(&config, target, adapters(), &history)
            .await
            .map_err(Into::into);
    }
    match command.as_slice() {
        [help] if help == "--help" || help == "-h" => {
            print_help();
            Ok(())
        }
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
            for target in Config::discover_targets(&config_path)? {
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
        [group, action, name] if group == "target" && action == "test" => {
            test_target(&config_path, name).await
        }
        [group, action, name] if group == "target" && action == "capabilities" => {
            show_capabilities(&config_path, name)
        }
        [serve] if serve == "serve" => serve_http(&config_path, "127.0.0.1:8088").await,
        [serve, bind, address] if serve == "serve" && bind == "--bind" => {
            serve_http(&config_path, address).await
        }
        _ => Err(AppError::Usage(
            "unknown command; run qcli --help for help".into(),
        )),
    }
}

async fn serve_http(path: &Path, address: &str) -> Result<(), AppError> {
    let address = address.parse().map_err(|error| {
        AppError::Usage(format!("invalid HTTP bind address '{address}': {error}"))
    })?;
    let listener = bind_local(address).await.map_err(AppError::Server)?;
    let service = HttpService::new(Config::load(path)?, adapters(), HttpLimits::default());
    eprintln!("qcli HTTP preview listening on http://{address}");
    tokio::select! {
        result = service.serve(listener) => result.map_err(AppError::Server),
        result = tokio::signal::ctrl_c() => result.map_err(AppError::Server),
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
    let started = Instant::now();
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
    let adapters = adapters();
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
    let mut progress = None;
    while let Some(event) = handle.next_event().await {
        match event {
            QueryEvent::EngineQueryId(id) => engine_query_id = Some(id),
            QueryEvent::Progress(current) => progress = Some(current),
            _ => {}
        }
    }
    handle.finish().await?;
    eprintln!("{rendered_rows} rows");
    eprintln!("Query ID: {query_id}");
    if let Some(id) = engine_query_id {
        eprintln!("Engine query ID: {id}");
    }
    if let Some(progress) = progress {
        if let (Some(completed), Some(total)) = (progress.completed_splits, progress.total_splits) {
            eprintln!("Splits: {completed}/{total}");
        }
        if let (Some(rows), Some(bytes)) = (progress.processed_rows, progress.processed_bytes) {
            eprintln!("Processed: {rows} rows, {bytes} bytes");
        }
    }
    eprintln!("Time: {:.3}s", started.elapsed().as_secs_f64());
    Ok(())
}

async fn test_target(path: &Path, target_name: &str) -> Result<(), AppError> {
    let config = Config::load(path)?;
    let target = config
        .target(target_name)
        .cloned()
        .ok_or_else(|| ConfigError {
            path: path.to_path_buf(),
            line: None,
            message: format!("target '{target_name}' does not exist"),
        })?;
    let engine = target.engine.clone();
    let snapshot = SessionManager::default().create(target);
    let service = QueryService::new(adapters(), 8);
    let mut handle = service.submit(snapshot, "SELECT 1".into())?;
    let mut rows = 0;
    while let Some(batch) = handle.next_batch().await {
        rows += batch.num_rows();
    }
    let mut remote_id = None;
    while let Some(event) = handle.next_event().await {
        if let QueryEvent::EngineQueryId(id) = event {
            remote_id = Some(id);
        }
    }
    handle.finish().await?;
    println!("Target '{target_name}' is reachable ({engine}, {rows} test row(s))");
    if let Some(id) = remote_id {
        println!("Engine query ID: {id}");
    }
    Ok(())
}

fn adapters() -> Vec<Arc<dyn EngineAdapter>> {
    vec![
        Arc::new(DemoAdapter),
        Arc::new(TrinoAdapter),
        Arc::new(DatabricksAdapter),
        Arc::new(SnowflakeAdapter),
    ]
}

fn show_capabilities(path: &Path, target_name: &str) -> Result<(), AppError> {
    let config = Config::load(path)?;
    let target = config.target(target_name).ok_or_else(|| ConfigError {
        path: path.to_path_buf(),
        line: None,
        message: format!("target '{target_name}' does not exist"),
    })?;
    let adapter = adapters()
        .into_iter()
        .find(|adapter| adapter.engine() == target.engine)
        .ok_or_else(|| CoreError::AdapterNotFound(target.engine.clone()))?;
    let capabilities = adapter.capabilities();
    println!("target = {target_name}");
    println!("engine = {}", target.engine);
    for capability in AdapterCapability::ALL {
        println!(
            "{} = {}",
            capability.as_str(),
            if capabilities.supports(capability) {
                "yes"
            } else {
                "no"
            }
        );
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
    println!("Usage: qcli [--config PATH] [--target TARGET]");
    println!("       qcli [--config PATH] <command>");
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
    println!("  target test NAME     Test target connectivity with SELECT 1");
    println!("  target capabilities NAME  Show supported engine capabilities");
    println!("  serve [--bind 127.0.0.1:PORT]  Start the local HTTP preview");
}
