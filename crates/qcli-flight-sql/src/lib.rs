//! Arrow Flight SQL transport for qcli's shared gateway service.

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandGetSqlInfo, ProstMessageExt, SqlInfo, SqlSupportedTransaction};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use futures_util::{Stream, TryStreamExt, stream};
use prost::Message;
use qcli_auth::{AuthenticatedPrincipal, AuthenticationErrorKind, Authenticator};
use qcli_service::{GatewayService, ServiceError, ServiceErrorKind};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

pub const FLIGHT_SERVICE_NAME: &str = "arrow.flight.protocol.FlightService";

#[derive(Debug, Clone)]
pub struct FlightTlsConfig {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FlightServerConfig {
    pub trusted_proxy: bool,
    pub max_message_bytes: usize,
    pub request_timeout: Duration,
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
    pub tls: Option<FlightTlsConfig>,
}

impl Default for FlightServerConfig {
    fn default() -> Self {
        Self {
            trusted_proxy: false,
            max_message_bytes: 16 * 1024 * 1024,
            request_timeout: Duration::from_secs(60),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            tls: None,
        }
    }
}

#[derive(Debug)]
pub enum FlightServerError {
    Io(std::io::Error),
    Transport(tonic::transport::Error),
    Configuration(String),
}

impl std::fmt::Display for FlightServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::Configuration(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for FlightServerError {}

impl From<std::io::Error> for FlightServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<tonic::transport::Error> for FlightServerError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error)
    }
}

/// Bind the configured Flight listener after enforcing the transport policy.
///
/// # Errors
///
/// Returns an error for a non-loopback plaintext listener without explicit
/// trusted-proxy mode, contradictory TLS/proxy settings, or a bind failure.
pub async fn bind_flight(
    address: SocketAddr,
    config: &FlightServerConfig,
) -> Result<TcpListener, FlightServerError> {
    if config.trusted_proxy && config.tls.is_some() {
        return Err(FlightServerError::Configuration(
            "Flight trusted-proxy mode and direct TLS are mutually exclusive".into(),
        ));
    }
    if !address.ip().is_loopback() && config.tls.is_none() && !config.trusted_proxy {
        return Err(FlightServerError::Configuration(
            "non-loopback Flight SQL requires direct TLS or --flight-trusted-proxy".into(),
        ));
    }
    TcpListener::bind(address).await.map_err(Into::into)
}

#[derive(Clone)]
pub struct QcliFlightSql {
    gateway: GatewayService,
    sql_info: Arc<SqlInfoData>,
}

impl QcliFlightSql {
    /// Build the minimal, honest M14 Flight SQL capability surface.
    ///
    /// # Panics
    ///
    /// Panics only if Arrow cannot construct its specification-defined SQL info
    /// record batch.
    #[must_use]
    pub fn new(gateway: GatewayService) -> Self {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "qcli");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
        builder.append(SqlInfo::FlightSqlServerReadOnly, false);
        builder.append(SqlInfo::FlightSqlServerSql, false);
        builder.append(SqlInfo::FlightSqlServerSubstrait, false);
        builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::None as i32,
        );
        builder.append(SqlInfo::FlightSqlServerCancel, false);
        builder.append(SqlInfo::FlightSqlServerBulkIngestion, false);
        Self {
            gateway,
            sql_info: Arc::new(builder.build().expect("static SQL info values are valid")),
        }
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayService {
        &self.gateway
    }
}

#[tonic::async_trait]
impl FlightSqlService for QcliFlightSql {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let bearer = bearer(request.metadata())?;
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Vec::new().into(),
        };
        let mut response = Response::new(Box::pin(stream::once(async { Ok(response) }))
            as Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>);
        response.metadata_mut().insert(
            "authorization",
            MetadataValue::from_str(&format!("Bearer {bearer}"))
                .map_err(|_| Status::internal("could not encode authorization metadata"))?,
        );
        Ok(response)
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(query.into_builder(&self.sql_info).schema().as_ref())
            .map_err(|error| Status::internal(error.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let builder = query.into_builder(&self.sql_info);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Serve Flight SQL and gRPC health until shutdown is requested.
///
/// # Errors
///
/// Returns certificate, key, or Tonic transport errors.
pub async fn serve_flight(
    listener: TcpListener,
    gateway: GatewayService,
    authenticator: Arc<dyn Authenticator>,
    config: FlightServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), FlightServerError> {
    let service = QcliFlightSql::new(gateway);
    let auth = authenticator.clone();
    let trusted_proxy = config.trusted_proxy;
    let interceptor = move |mut request: Request<()>| {
        enforce_proxy_policy(&request, trusted_proxy)?;
        let credential = bearer(request.metadata())?;
        let principal = auth
            .authenticate_immediate(credential)
            .map_err(authentication_status)?;
        request.extensions_mut().insert(principal);
        Ok(request)
    };
    let flight = FlightServiceServer::new(service)
        .max_decoding_message_size(config.max_message_bytes)
        .max_encoding_message_size(config.max_message_bytes);
    let flight = InterceptedService::new(flight, interceptor);

    let (health_reporter, health) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status(FLIGHT_SERVICE_NAME, tonic_health::ServingStatus::Serving)
        .await;

    let mut server = Server::builder()
        .timeout(config.request_timeout)
        .http2_keepalive_interval(Some(config.keepalive_interval))
        .http2_keepalive_timeout(Some(config.keepalive_timeout))
        .tcp_keepalive(Some(config.keepalive_interval));
    if let Some(tls) = &config.tls {
        let certificate = std::fs::read(&tls.certificate).map_err(|error| {
            FlightServerError::Configuration(format!(
                "{}: cannot read Flight TLS certificate: {error}",
                tls.certificate.display()
            ))
        })?;
        let private_key = std::fs::read(&tls.private_key).map_err(|error| {
            FlightServerError::Configuration(format!(
                "{}: cannot read Flight TLS private key: {error}",
                tls.private_key.display()
            ))
        })?;
        server = server.tls_config(
            ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key)),
        )?;
    }
    let health_shutdown = async move {
        shutdown.await;
        health_reporter
            .set_service_status(FLIGHT_SERVICE_NAME, tonic_health::ServingStatus::NotServing)
            .await;
    };
    server
        .add_service(health)
        .add_service(flight)
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), health_shutdown)
        .await?;
    Ok(())
}

fn bearer(metadata: &tonic::metadata::MetadataMap) -> Result<&str, Status> {
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unauthenticated("a bearer credential is required"))
}

fn enforce_proxy_policy<T>(request: &Request<T>, trusted_proxy: bool) -> Result<(), Status> {
    let forwarded = request.metadata().contains_key("forwarded")
        || request.metadata().contains_key("x-forwarded-for")
        || request.metadata().contains_key("x-forwarded-host")
        || request.metadata().contains_key("x-forwarded-proto");
    if forwarded && !trusted_proxy {
        return Err(Status::permission_denied(
            "forwarded metadata requires Flight trusted-proxy mode",
        ));
    }
    if trusted_proxy
        && request
            .metadata()
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            != Some("https")
    {
        return Err(Status::failed_precondition(
            "trusted Flight proxy must report x-forwarded-proto: https",
        ));
    }
    Ok(())
}

fn authentication_status(error: qcli_auth::AuthenticationError) -> Status {
    match error.kind {
        AuthenticationErrorKind::Configuration => Status::internal(error.message),
        AuthenticationErrorKind::Missing | AuthenticationErrorKind::Invalid => {
            Status::unauthenticated(error.message)
        }
    }
}

#[must_use]
pub fn service_error_status(error: ServiceError) -> Status {
    match error.kind {
        ServiceErrorKind::InvalidArgument => Status::invalid_argument(error.message),
        ServiceErrorKind::NotFound => Status::not_found(error.message),
        ServiceErrorKind::Forbidden => Status::permission_denied(error.message),
        ServiceErrorKind::Conflict => Status::aborted(error.message),
        ServiceErrorKind::ResourceExhausted => Status::resource_exhausted(error.message),
        ServiceErrorKind::FailedPrecondition => Status::failed_precondition(error.message),
        ServiceErrorKind::Upstream => Status::unavailable(error.message),
        ServiceErrorKind::Internal => Status::internal(error.message),
    }
}

#[must_use]
pub fn principal(request: &Request<impl Sized>) -> Option<&AuthenticatedPrincipal> {
    request.extensions().get::<AuthenticatedPrincipal>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use qcli_auth::AuthenticationError;
    use qcli_config::Config;
    use qcli_driver_api::EngineAdapter;
    use qcli_driver_demo::DemoAdapter;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tonic::Code;
    use tonic::transport::{Channel, Endpoint};

    static NEXT_CONFIG: AtomicU64 = AtomicU64::new(1);

    struct StaticAuthenticator;

    #[async_trait::async_trait]
    impl Authenticator for StaticAuthenticator {
        fn authenticate_immediate(
            &self,
            bearer: &str,
        ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
            if bearer != "valid-key" {
                return Err(AuthenticationError {
                    kind: AuthenticationErrorKind::Invalid,
                    message: "invalid bearer credential".into(),
                });
            }
            Ok(AuthenticatedPrincipal {
                id: "analyst".into(),
                allowed_targets: BTreeSet::from(["demo".into()]),
                max_sessions: 2,
                max_concurrent_queries: 2,
            })
        }
    }

    fn gateway() -> GatewayService {
        let id = NEXT_CONFIG.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("qcli-flight-sql-{}-{id}.env", std::process::id()));
        std::fs::write(&path, "[demo]\nengine=demo\n").unwrap();
        let config = Config::load(&path).unwrap();
        std::fs::remove_file(path).ok();
        GatewayService::new(
            config,
            [Arc::new(DemoAdapter) as Arc<dyn EngineAdapter>],
            qcli_service::ServiceLimits::default(),
        )
    }

    async fn server(
        config: FlightServerConfig,
    ) -> (
        FlightSqlServiceClient<Channel>,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), FlightServerError>>,
    ) {
        let listener = bind_flight("127.0.0.1:0".parse().unwrap(), &config)
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve_flight(
            listener,
            gateway(),
            Arc::new(StaticAuthenticator),
            config,
            async {
                shutdown_rx.await.ok();
            },
        ));
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (FlightSqlServiceClient::new(channel), shutdown_tx, task)
    }

    #[tokio::test]
    async fn authenticated_client_discovers_and_reads_sql_info() {
        let (mut client, shutdown, task) = server(FlightServerConfig::default()).await;
        client.set_token("valid-key".into());
        let info = client
            .get_sql_info(vec![
                SqlInfo::FlightSqlServerName,
                SqlInfo::FlightSqlServerSql,
                SqlInfo::FlightSqlServerTransaction,
            ])
            .await
            .unwrap();
        assert_eq!(info.endpoint.len(), 1);
        let mut results = client
            .do_get(info.endpoint[0].ticket.clone().unwrap())
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = results.try_next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3);
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authentication_and_unsupported_operations_fail_stably() {
        let (mut client, shutdown, task) = server(FlightServerConfig::default()).await;
        let error = client.get_sql_info(vec![]).await.unwrap_err();
        assert!(matches!(
            error,
            arrow_flight::error::FlightError::Tonic(ref status)
                if status.code() == Code::Unauthenticated
        ));

        client.set_token("valid-key".into());
        let error = client.execute("select 1".into(), None).await.unwrap_err();
        assert!(matches!(
            error,
            arrow_flight::error::FlightError::Tonic(ref status)
                if status.code() == Code::Unimplemented
        ));
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn health_is_available_without_database_credentials() {
        let config = FlightServerConfig::default();
        let listener = bind_flight("127.0.0.1:0".parse().unwrap(), &config)
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve_flight(
            listener,
            gateway(),
            Arc::new(StaticAuthenticator),
            config,
            async {
                shutdown_rx.await.ok();
            },
        ));
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut health = tonic_health::pb::health_client::HealthClient::new(channel);
        let response = health
            .check(tonic_health::pb::HealthCheckRequest {
                service: FLIGHT_SERVICE_NAME.into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.status,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        );
        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn proxy_and_message_policies_fail_closed() {
        let config = FlightServerConfig {
            max_message_bytes: 128,
            ..FlightServerConfig::default()
        };
        let (mut client, shutdown, task) = server(config).await;
        client.set_token("valid-key".into());
        let error = client.execute("x".repeat(1024), None).await.unwrap_err();
        assert!(matches!(
            error,
            arrow_flight::error::FlightError::Tonic(ref status)
                if status.code() == Code::OutOfRange || status.code() == Code::ResourceExhausted
        ));
        client.set_header("x-forwarded-proto", "https");
        let error = client.get_sql_info(vec![]).await.unwrap_err();
        assert!(matches!(
            error,
            arrow_flight::error::FlightError::Tonic(ref status)
                if status.code() == Code::PermissionDenied
        ));
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn unsafe_listener_and_invalid_tls_configuration_are_rejected() {
        let error = bind_flight("0.0.0.0:0".parse().unwrap(), &FlightServerConfig::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("requires direct TLS"));

        let error = bind_flight(
            "127.0.0.1:0".parse().unwrap(),
            &FlightServerConfig {
                trusted_proxy: true,
                tls: Some(FlightTlsConfig {
                    certificate: "missing.pem".into(),
                    private_key: "missing.key".into(),
                }),
                ..FlightServerConfig::default()
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));

        let config = FlightServerConfig {
            tls: Some(FlightTlsConfig {
                certificate: "definitely-missing.pem".into(),
                private_key: "definitely-missing.key".into(),
            }),
            ..FlightServerConfig::default()
        };
        let listener = bind_flight("127.0.0.1:0".parse().unwrap(), &config)
            .await
            .unwrap();
        let error = serve_flight(
            listener,
            gateway(),
            Arc::new(StaticAuthenticator),
            config,
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot read Flight TLS certificate")
        );
    }
}
