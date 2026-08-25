use qcli_auth::{
    ApiKeyAuthenticator, AuthenticationError, Authenticator, CompositeAuthenticator,
    OidcAuthenticator, generate_api_key_material,
};
use qcli_cluster::{
    ClusterStateStore, PostgresClusterStateStore, ResultObjectStore, SharedObjectStore,
};
use qcli_config::{Config, ConfigError, ResolvedTarget, default_config_path};
use qcli_core::{CoreError, QueryService, SessionManager};
use qcli_driver_api::{AdapterCapability, EngineAdapter, QueryEvent};
use qcli_driver_databricks::DatabricksAdapter;
use qcli_driver_demo::DemoAdapter;
use qcli_driver_snowflake::SnowflakeAdapter;
use qcli_driver_trino::TrinoAdapter;
use qcli_flight_sql::{
    FlightServerConfig, FlightServerError, FlightTlsConfig, bind_flight, serve_flight,
};
use qcli_http::{HttpLimits, HttpOperations, HttpService, bind_http, stderr_audit_sink};
use qcli_output::{DisplayOptions, OutputError, OutputFormat, StreamOutput};
use qcli_repl::ReplError;
use qcli_service::{GatewayService, ServiceLimits};
use std::env;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
enum AppError {
    Usage(String),
    Config(ConfigError),
    Query(CoreError),
    Input(io::Error),
    Output(OutputError),
    Repl(ReplError),
    Server(io::Error),
    Flight(FlightServerError),
    Authentication(AuthenticationError),
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
            Self::Server(_) | Self::Flight(_) => 8,
            Self::Authentication(_) => 9,
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
            Self::Flight(error) => write!(f, "Flight SQL service failed: {error}"),
            Self::Authentication(error) => write!(f, "authentication failed: {error}"),
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

impl From<AuthenticationError> for AppError {
    fn from(value: AuthenticationError) -> Self {
        Self::Authentication(value)
    }
}

impl From<FlightServerError> for AppError {
    fn from(value: FlightServerError) -> Self {
        Self::Flight(value)
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
    if command.first().is_some_and(|value| value == "serve") {
        return serve_gateway(&config_path, parse_serve_args(&command[1..])?).await;
    }
    match command.as_slice() {
        [version] if version == "--version" || version == "-V" => {
            println!("qcli {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
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
        [group, resource, action, key_id]
            if group == "auth" && resource == "key" && action == "create" =>
        {
            create_api_key(key_id)
        }
        _ => Err(AppError::Usage(
            "unknown command; run qcli --help for help".into(),
        )),
    }
}

fn create_api_key(key_id: &str) -> Result<(), AppError> {
    let (key, hash) = generate_api_key_material(key_id)?;
    println!("API key (shown once): {}", key.expose());
    println!("secret_hash = \"{hash}\"");
    Ok(())
}

struct ServeArguments {
    address: String,
    flight_address: Option<String>,
    auth_file: Option<PathBuf>,
    oidc_file: Option<PathBuf>,
    trusted_proxy: bool,
    flight_trusted_proxy: bool,
    flight_tls_certificate: Option<PathBuf>,
    flight_tls_private_key: Option<PathBuf>,
    flight_tls_client_ca: Option<PathBuf>,
    allowed_origins: Vec<String>,
    cluster_url: Option<String>,
    node_id: Option<String>,
    result_store_url: Option<String>,
    flight_signing_key: Option<PathBuf>,
}

fn parse_serve_args(arguments: &[String]) -> Result<ServeArguments, AppError> {
    let mut result = ServeArguments {
        address: "127.0.0.1:8088".into(),
        flight_address: None,
        auth_file: None,
        oidc_file: None,
        trusted_proxy: false,
        flight_trusted_proxy: false,
        flight_tls_certificate: None,
        flight_tls_private_key: None,
        flight_tls_client_ca: None,
        allowed_origins: Vec::new(),
        cluster_url: None,
        node_id: None,
        result_store_url: None,
        flight_signing_key: None,
    };
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--trusted-proxy" if !result.trusted_proxy => {
                result.trusted_proxy = true;
                index += 1;
            }
            "--flight-trusted-proxy" if !result.flight_trusted_proxy => {
                result.flight_trusted_proxy = true;
                index += 1;
            }
            "--bind"
            | "--flight-bind"
            | "--flight-tls-cert"
            | "--flight-tls-key"
            | "--flight-tls-client-ca"
            | "--auth-file"
            | "--oidc-file"
            | "--cluster-url"
            | "--node-id"
            | "--result-store-url"
            | "--flight-signing-key"
            | "--cors-origin" => {
                let flag = &arguments[index];
                let value = arguments
                    .get(index + 1)
                    .ok_or_else(|| AppError::Usage(format!("{flag} requires a value")))?;
                match flag.as_str() {
                    "--bind" => result.address.clone_from(value),
                    "--flight-bind" => result.flight_address = Some(value.clone()),
                    "--flight-tls-cert" => {
                        result.flight_tls_certificate = Some(PathBuf::from(value));
                    }
                    "--flight-tls-key" => {
                        result.flight_tls_private_key = Some(PathBuf::from(value));
                    }
                    "--flight-tls-client-ca" => {
                        result.flight_tls_client_ca = Some(PathBuf::from(value));
                    }
                    "--auth-file" => result.auth_file = Some(PathBuf::from(value)),
                    "--oidc-file" => result.oidc_file = Some(PathBuf::from(value)),
                    "--cluster-url" => result.cluster_url = Some(value.clone()),
                    "--node-id" => result.node_id = Some(value.clone()),
                    "--result-store-url" => result.result_store_url = Some(value.clone()),
                    "--flight-signing-key" => {
                        result.flight_signing_key = Some(PathBuf::from(value));
                    }
                    "--cors-origin" => result.allowed_origins.push(value.clone()),
                    _ => unreachable!(),
                }
                index += 2;
            }
            flag => return Err(AppError::Usage(format!("unknown serve option '{flag}'"))),
        }
    }
    Ok(result)
}

fn flight_server_config(
    arguments: &ServeArguments,
    has_authenticator: bool,
) -> Result<(Option<std::net::SocketAddr>, FlightServerConfig), AppError> {
    let address = arguments
        .flight_address
        .as_deref()
        .map(str::parse::<std::net::SocketAddr>)
        .transpose()
        .map_err(|error| AppError::Usage(format!("invalid Flight bind address: {error}")))?;
    let tls = match (
        &arguments.flight_tls_certificate,
        &arguments.flight_tls_private_key,
    ) {
        (Some(certificate), Some(private_key)) => Some(FlightTlsConfig {
            certificate: certificate.clone(),
            private_key: private_key.clone(),
            client_ca: arguments.flight_tls_client_ca.clone(),
        }),
        (None, None) => None,
        _ => {
            return Err(AppError::Usage(
                "--flight-tls-cert and --flight-tls-key must be supplied together".into(),
            ));
        }
    };
    if arguments.flight_tls_client_ca.is_some() && tls.is_none() {
        return Err(AppError::Usage(
            "--flight-tls-client-ca requires --flight-tls-cert and --flight-tls-key".into(),
        ));
    }
    if address.is_some() && !has_authenticator {
        return Err(AppError::Usage(
            "--flight-bind requires --auth-file or --oidc-file".into(),
        ));
    }
    Ok((
        address,
        FlightServerConfig {
            trusted_proxy: arguments.flight_trusted_proxy,
            tls,
            shared_signing_key: arguments
                .flight_signing_key
                .clone()
                .or_else(|| env::var_os("QCLI_FLIGHT_SIGNING_KEY").map(PathBuf::from))
                .as_deref()
                .map(read_signing_key)
                .transpose()?,
            ..FlightServerConfig::default()
        },
    ))
}

fn read_signing_key(path: &Path) -> Result<[u8; 32], AppError> {
    let bytes = fs::read(path).map_err(AppError::Input)?;
    bytes
        .try_into()
        .map_err(|_| AppError::Usage("Flight signing key must contain exactly 32 bytes".into()))
}

async fn initialize_cluster(
    arguments: &ServeArguments,
) -> Result<
    Option<(
        Arc<PostgresClusterStateStore>,
        Arc<dyn ResultObjectStore>,
        String,
    )>,
    AppError,
> {
    let cluster_url = arguments
        .cluster_url
        .clone()
        .or_else(|| env::var("QCLI_CLUSTER_URL").ok());
    let result_store_url = arguments
        .result_store_url
        .clone()
        .or_else(|| env::var("QCLI_RESULT_STORE_URL").ok());
    let signing_key =
        arguments.flight_signing_key.is_some() || env::var_os("QCLI_FLIGHT_SIGNING_KEY").is_some();
    let cluster = if let Some(url) = cluster_url.as_deref() {
        if arguments.flight_address.is_some() && !signing_key {
            return Err(AppError::Usage(
                "clustered Flight SQL requires --flight-signing-key or QCLI_FLIGHT_SIGNING_KEY"
                    .into(),
            ));
        }
        let node_id = arguments
            .node_id
            .clone()
            .or_else(|| env::var("QCLI_NODE_ID").ok())
            .unwrap_or_else(|| format!("qcli-{}", std::process::id()));
        let store = Arc::new(
            PostgresClusterStateStore::connect(url)
                .await
                .map_err(|error| AppError::Usage(format!("cluster store: {error}")))?,
        );
        store
            .migrate()
            .await
            .map_err(|error| AppError::Usage(format!("cluster migration: {error}")))?;
        store
            .register_node(
                &node_id,
                env!("CARGO_PKG_VERSION"),
                &["http".into(), "flight-sql".into()],
                Duration::from_secs(30),
            )
            .await
            .map_err(|error| AppError::Usage(format!("cluster registration: {error}")))?;
        let result_url = result_store_url.as_deref().ok_or_else(|| {
            AppError::Usage(
                "cluster mode requires --result-store-url or QCLI_RESULT_STORE_URL".into(),
            )
        })?;
        let objects = Arc::new(
            SharedObjectStore::from_url(result_url)
                .map_err(|error| AppError::Usage(format!("result store: {error}")))?,
        ) as Arc<dyn ResultObjectStore>;
        Some((store, objects, node_id))
    } else {
        if arguments.node_id.is_some() || arguments.result_store_url.is_some() {
            return Err(AppError::Usage(
                "--node-id and --result-store-url require --cluster-url".into(),
            ));
        }
        None
    };
    Ok(cluster)
}

#[allow(
    clippy::too_many_lines,
    reason = "composes the two serve frontends and shared shutdown lifecycle"
)]
async fn serve_gateway(path: &Path, arguments: ServeArguments) -> Result<(), AppError> {
    let cluster = initialize_cluster(&arguments).await?;
    let address = arguments.address.parse().map_err(|error| {
        AppError::Usage(format!(
            "invalid HTTP bind address '{}': {error}",
            arguments.address
        ))
    })?;
    let mut providers = Vec::<Arc<dyn Authenticator>>::new();
    if let Some(path) = arguments.auth_file.as_deref() {
        providers.push(Arc::new(ApiKeyAuthenticator::load(path)?));
    }
    if let Some(path) = arguments.oidc_file.as_deref() {
        providers.push(Arc::new(OidcAuthenticator::load(path)?));
    }
    let authenticator = (!providers.is_empty())
        .then(|| Arc::new(CompositeAuthenticator::new(providers)) as Arc<dyn Authenticator>);
    let (flight, flight_config) = flight_server_config(&arguments, authenticator.is_some())?;
    let listener = bind_http(address, arguments.trusted_proxy, authenticator.is_some())
        .await
        .map_err(AppError::Server)?;
    let flight_listener = if let Some(flight_address) = flight {
        Some(bind_flight(flight_address, &flight_config).await?)
    } else {
        None
    };

    let mut gateway =
        GatewayService::new(Config::load(path)?, adapters(), ServiceLimits::default());
    let mut cluster_drainer = None;
    if let Some((store, objects, node_id)) = cluster {
        gateway = gateway.with_cluster(
            node_id.clone(),
            store.clone(),
            objects,
            Duration::from_secs(30),
        );
        cluster_drainer = Some((store.clone(), node_id.clone()));
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if store
                    .renew_node(&node_id, Duration::from_secs(30))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
    }
    let mut service = HttpService::from_gateway(gateway.clone(), HttpLimits::default());
    if let Some(authenticator) = &authenticator {
        service = service
            .with_authenticator(authenticator.clone())
            .with_audit_sink(stderr_audit_sink());
    }
    service = service.with_operations(HttpOperations {
        trusted_proxy: arguments.trusted_proxy,
        allowed_origins: arguments.allowed_origins,
    });
    eprintln!("qcli HTTP service listening on http://{address}");
    let Some(flight_listener) = flight_listener else {
        let signal_gateway = gateway.clone();
        return service
            .serve_with_shutdown(listener, async move {
                tokio::signal::ctrl_c().await.ok();
                if let Some((store, node_id)) = cluster_drainer {
                    store.set_draining(&node_id, true).await.ok();
                }
                signal_gateway.begin_shutdown();
            })
            .await
            .map_err(AppError::Server);
    };
    let flight_address = flight_listener.local_addr().map_err(AppError::Server)?;
    eprintln!("qcli Flight SQL service listening on {flight_address}");
    let authenticator = authenticator.expect("Flight authentication was validated");
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let mut http_shutdown = shutdown_tx.subscribe();
    let mut flight_shutdown = shutdown_tx.subscribe();
    let signal_gateway = gateway.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        if let Some((store, node_id)) = cluster_drainer {
            store.set_draining(&node_id, true).await.ok();
        }
        signal_gateway.begin_shutdown();
        shutdown_tx.send(()).ok();
    });
    let http = async move {
        service
            .serve_with_shutdown(listener, async move {
                http_shutdown.recv().await.ok();
            })
            .await
            .map_err(AppError::Server)
    };
    let flight = async move {
        serve_flight(
            flight_listener,
            gateway,
            authenticator,
            flight_config,
            async move {
                flight_shutdown.recv().await.ok();
            },
        )
        .await
        .map_err(AppError::Flight)
    };
    tokio::try_join!(http, flight)?;
    Ok(())
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
    println!("  --version            Print qcli version");
    println!("  config path          Print the configuration path");
    println!("  config check         Validate configuration and targets");
    println!("  config show          Show resolved configuration with secrets redacted");
    println!("  target list          List configured targets");
    println!("  target show NAME     Show one resolved target with secrets redacted");
    println!("  target test NAME     Test target connectivity with SELECT 1");
    println!("  target capabilities NAME  Show supported engine capabilities");
    println!("  auth key create ID   Generate an API key and Argon2id hash");
    println!(
        "  serve [--bind ADDRESS] [--auth-file PATH] [--oidc-file PATH] [--trusted-proxy] [--cors-origin ORIGIN]"
    );
    println!("        [--flight-bind ADDRESS] [--flight-tls-cert PATH --flight-tls-key PATH]");
    println!("        [--flight-trusted-proxy]");
    println!("        [--flight-tls-cert PATH --flight-tls-key PATH --flight-tls-client-ca PATH]");
    println!("        [--cluster-url POSTGRES_URL --node-id ID --result-store-url URL]");
    println!("        [--flight-signing-key 32_BYTE_FILE]");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    struct TestAuthenticator;

    impl Authenticator for TestAuthenticator {
        fn authenticate_immediate(
            &self,
            _bearer: &str,
        ) -> Result<qcli_auth::AuthenticatedPrincipal, AuthenticationError> {
            Ok(qcli_auth::AuthenticatedPrincipal {
                id: "test".into(),
                allowed_targets: BTreeSet::from(["demo".into()]),
                max_sessions: 2,
                max_concurrent_queries: 2,
            })
        }
    }

    #[test]
    fn flight_serve_arguments_are_explicit_and_complete() {
        let arguments = parse_serve_args(&[
            "--flight-bind".into(),
            "127.0.0.1:32010".into(),
            "--auth-file".into(),
            "auth.toml".into(),
            "--oidc-file".into(),
            "oidc.toml".into(),
            "--flight-tls-cert".into(),
            "server.pem".into(),
            "--flight-tls-key".into(),
            "server.key".into(),
            "--flight-tls-client-ca".into(),
            "client-ca.pem".into(),
            "--cluster-url".into(),
            "postgresql://cluster".into(),
            "--node-id".into(),
            "node-a".into(),
            "--result-store-url".into(),
            "s3://qcli-results/test".into(),
        ])
        .unwrap();
        assert_eq!(arguments.flight_address.as_deref(), Some("127.0.0.1:32010"));
        assert_eq!(
            arguments.flight_tls_certificate.as_deref(),
            Some(Path::new("server.pem"))
        );
        assert_eq!(
            arguments.flight_tls_private_key.as_deref(),
            Some(Path::new("server.key"))
        );
        assert_eq!(arguments.oidc_file.as_deref(), Some(Path::new("oidc.toml")));
        assert_eq!(arguments.node_id.as_deref(), Some("node-a"));
        let (_, config) = flight_server_config(&arguments, true).unwrap();
        assert_eq!(
            config.tls.unwrap().client_ca.as_deref(),
            Some(Path::new("client-ca.pem"))
        );
    }

    #[test]
    fn flight_listener_requires_auth_and_complete_tls_identity() {
        let no_auth =
            parse_serve_args(&["--flight-bind".into(), "127.0.0.1:32010".into()]).unwrap();
        assert!(flight_server_config(&no_auth, false).is_err());

        let incomplete_tls = parse_serve_args(&[
            "--flight-bind".into(),
            "127.0.0.1:32010".into(),
            "--flight-tls-cert".into(),
            "server.pem".into(),
        ])
        .unwrap();
        assert!(flight_server_config(&incomplete_tls, true).is_err());

        let client_ca_without_tls = parse_serve_args(&[
            "--flight-bind".into(),
            "127.0.0.1:32010".into(),
            "--flight-tls-client-ca".into(),
            "client-ca.pem".into(),
        ])
        .unwrap();
        assert!(flight_server_config(&client_ca_without_tls, true).is_err());
    }

    #[tokio::test]
    async fn http_and_flight_start_and_shutdown_on_one_runtime() {
        let path = std::env::temp_dir().join(format!("qcli-m14-{}.env", std::process::id()));
        std::fs::write(&path, "[demo]\nengine=demo\n").unwrap();
        let gateway = GatewayService::new(
            Config::load(&path).unwrap(),
            [Arc::new(DemoAdapter) as Arc<dyn EngineAdapter>],
            ServiceLimits::default(),
        );
        std::fs::remove_file(path).ok();

        let http_listener = bind_http("127.0.0.1:0".parse().unwrap(), false, false)
            .await
            .unwrap();
        let http_address = http_listener.local_addr().unwrap();
        let flight_config = FlightServerConfig::default();
        let flight_listener = bind_flight("127.0.0.1:0".parse().unwrap(), &flight_config)
            .await
            .unwrap();
        let flight_address = flight_listener.local_addr().unwrap();
        let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
        let (flight_shutdown_tx, flight_shutdown_rx) = tokio::sync::oneshot::channel();

        let http_gateway = gateway.clone();
        let http = tokio::spawn(async move {
            HttpService::from_gateway(http_gateway, HttpLimits::default())
                .serve_with_shutdown(http_listener, async {
                    http_shutdown_rx.await.ok();
                })
                .await
        });
        let flight = tokio::spawn(serve_flight(
            flight_listener,
            gateway,
            Arc::new(TestAuthenticator),
            flight_config,
            async {
                flight_shutdown_rx.await.ok();
            },
        ));

        tokio::net::TcpStream::connect(http_address).await.unwrap();
        tokio::net::TcpStream::connect(flight_address)
            .await
            .unwrap();
        http_shutdown_tx.send(()).unwrap();
        flight_shutdown_tx.send(()).unwrap();
        http.await.unwrap().unwrap();
        flight.await.unwrap().unwrap();
    }
}
