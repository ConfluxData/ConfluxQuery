//! Arrow Flight SQL transport for qcli's shared gateway service.

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{
    SqlInfoData, SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder,
};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionCancelQueryRequest, ActionCancelQueryResult, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandStatementQuery, Nullable, ProstMessageExt, Searchable, SqlInfo,
    SqlSupportedCaseSensitivity, SqlSupportedTransaction, TicketStatementQuery, XdbcDataType,
};
use arrow_flight::{
    Action, ActionType, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, Result as FlightActionResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::{Stream, TryStreamExt, stream};
use hmac::{Hmac, Mac};
use prost::Message;
use qcli_auth::{AuthenticatedPrincipal, AuthenticationErrorKind, Authenticator};
use qcli_driver_api::{
    AdapterCapability, ColumnMetadata, IdentifierCapabilities, IdentifierCase, MetadataRequest,
    ObjectKind,
};
use qcli_service::{GatewayService, ResultBatchReader, ServiceError, ServiceErrorKind};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

pub const FLIGHT_SERVICE_NAME: &str = "arrow.flight.protocol.FlightService";
const SESSION_COOKIE: &str = "arrow_flight_session_id";
const SET_SESSION_OPTIONS: &str = "SetSessionOptions";
const GET_SESSION_OPTIONS: &str = "GetSessionOptions";
const CLOSE_SESSION: &str = "CloseSession";

mod session_proto {
    use std::collections::HashMap;

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct SessionOptionValue {
        #[prost(oneof = "session_option_value::OptionValue", tags = "1, 2, 3, 4, 5")]
        pub option_value: Option<session_option_value::OptionValue>,
    }

    pub mod session_option_value {
        #[derive(Clone, PartialEq, prost::Message)]
        pub struct StringListValue {
            #[prost(string, repeated, tag = "1")]
            pub values: Vec<String>,
        }

        #[derive(Clone, PartialEq, prost::Oneof)]
        #[allow(clippy::enum_variant_names)]
        pub enum OptionValue {
            #[prost(string, tag = "1")]
            StringValue(String),
            #[prost(bool, tag = "2")]
            BoolValue(bool),
            #[prost(sfixed64, tag = "3")]
            Int64Value(i64),
            #[prost(double, tag = "4")]
            DoubleValue(f64),
            #[prost(message, tag = "5")]
            StringListValue(StringListValue),
        }
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct SetSessionOptionsRequest {
        #[prost(map = "string, message", tag = "1")]
        pub session_options: HashMap<String, SessionOptionValue>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct SetSessionOptionsResult {
        #[prost(map = "string, message", tag = "1")]
        pub errors: HashMap<String, SetSessionOptionError>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct SetSessionOptionError {
        #[prost(enumeration = "SetSessionOptionErrorValue", tag = "1")]
        pub value: i32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    pub enum SetSessionOptionErrorValue {
        Unspecified = 0,
        InvalidName = 1,
        InvalidValue = 2,
        Error = 3,
    }

    #[derive(Clone, Copy, PartialEq, prost::Message)]
    pub struct GetSessionOptionsRequest {}

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct GetSessionOptionsResult {
        #[prost(map = "string, message", tag = "1")]
        pub session_options: HashMap<String, SessionOptionValue>,
    }

    #[derive(Clone, Copy, PartialEq, prost::Message)]
    pub struct CloseSessionRequest {}

    #[derive(Clone, Copy, PartialEq, prost::Message)]
    pub struct CloseSessionResult {
        #[prost(enumeration = "CloseSessionStatus", tag = "1")]
        pub status: i32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    pub enum CloseSessionStatus {
        Unspecified = 0,
        Closed = 1,
        Closing = 2,
        NotCloseable = 3,
    }
}

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
    pub ticket_ttl: Duration,
    pub session_ttl: Duration,
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
            ticket_ttl: Duration::from_secs(15 * 60),
            session_ttl: Duration::from_secs(30 * 60),
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
    xdbc_info: Arc<XdbcTypeInfoData>,
    tickets: Arc<TicketSigner>,
    sessions: Arc<SessionSigner>,
    ticket_ttl: Duration,
    session_ttl: Duration,
    max_flight_data_bytes: usize,
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
        Self::with_limits(
            gateway,
            Duration::from_secs(15 * 60),
            Duration::from_secs(30 * 60),
            16 * 1024 * 1024,
        )
    }

    #[must_use]
    /// Build a Flight SQL service with explicit ticket and message limits.
    ///
    /// # Panics
    ///
    /// Panics only if Arrow cannot construct its specification-defined SQL
    /// information record batch from static values.
    pub fn with_limits(
        gateway: GatewayService,
        ticket_ttl: Duration,
        session_ttl: Duration,
        max_flight_data_bytes: usize,
    ) -> Self {
        Self {
            gateway,
            sql_info: Arc::new(sql_info_data(true, &IdentifierCapabilities::default())),
            xdbc_info: Arc::new(xdbc_type_info()),
            tickets: Arc::new(TicketSigner::new()),
            sessions: Arc::new(SessionSigner::new()),
            ticket_ttl,
            session_ttl,
            max_flight_data_bytes: max_flight_data_bytes.max(1024),
        }
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayService {
        &self.gateway
    }

    fn session_from_request<T>(
        &self,
        request: &Request<T>,
        principal: &AuthenticatedPrincipal,
    ) -> Result<Option<SessionTokenPayload>, Status> {
        let Some(token) = cookie(request, SESSION_COOKIE) else {
            return Ok(None);
        };
        let payload = self.sessions.verify(token.as_bytes(), &principal.id)?;
        let snapshot = self
            .gateway
            .session(principal, &payload.session_id)
            .map_err(service_error_status)?;
        if snapshot.version != payload.session_version {
            return Err(Status::aborted(
                "Flight session token has a stale session version",
            ));
        }
        Ok(Some(payload))
    }

    fn set_session_cookie<T>(
        &self,
        response: &mut Response<T>,
        principal: &AuthenticatedPrincipal,
        snapshot: &qcli_core::SessionSnapshot,
    ) -> Result<(), Status> {
        let token = self.sessions.issue(
            &principal.id,
            &snapshot.id,
            snapshot.version,
            self.session_ttl,
        )?;
        response.metadata_mut().insert(
            "set-cookie",
            MetadataValue::from_str(&format!(
                "{SESSION_COOKIE}={}; Path=/; HttpOnly; SameSite=Strict",
                String::from_utf8_lossy(&token)
            ))
            .map_err(|_| Status::internal("could not encode Flight session cookie"))?,
        );
        Ok(())
    }

    fn set_session_options(
        &self,
        request: &Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        use session_proto::{
            SetSessionOptionError, SetSessionOptionErrorValue, SetSessionOptionsRequest,
            SetSessionOptionsResult,
        };

        let principal = required_principal(request)?.clone();
        let current = self.session_from_request(request, &principal)?;
        let input = SetSessionOptionsRequest::decode(request.get_ref().body.as_ref())
            .map_err(|_| Status::invalid_argument("SetSessionOptions body is malformed"))?;
        let mut target = None;
        let mut overrides = BTreeMap::new();
        let mut errors = std::collections::HashMap::new();
        for (name, value) in input.session_options {
            if matches!(name.as_str(), "qcli.version" | "qcli.session_id") {
                errors.insert(
                    name,
                    SetSessionOptionError {
                        value: SetSessionOptionErrorValue::InvalidName as i32,
                    },
                );
                continue;
            }
            let Some(value) = session_option_string(value) else {
                errors.insert(
                    name,
                    SetSessionOptionError {
                        value: SetSessionOptionErrorValue::InvalidValue as i32,
                    },
                );
                continue;
            };
            if name == "qcli.target" {
                match value {
                    ParsedSessionOption::Value(value) if !value.is_empty() => target = Some(value),
                    _ => {
                        errors.insert(
                            name,
                            SetSessionOptionError {
                                value: SetSessionOptionErrorValue::InvalidValue as i32,
                            },
                        );
                    }
                }
            } else if let Some(property) = session_property_name(&name) {
                overrides.insert(
                    property,
                    match value {
                        ParsedSessionOption::Unset => None,
                        ParsedSessionOption::Value(value) => Some(value),
                    },
                );
            } else {
                errors.insert(
                    name,
                    SetSessionOptionError {
                        value: SetSessionOptionErrorValue::InvalidName as i32,
                    },
                );
            }
        }

        let snapshot = if let Some(current) = current {
            self.gateway
                .mutate_session(
                    &principal,
                    &current.session_id,
                    current.session_version,
                    target.as_deref(),
                    overrides,
                )
                .map_err(service_error_status)?
        } else {
            let target = target.ok_or_else(|| {
                Status::invalid_argument("qcli.target is required when creating a Flight session")
            })?;
            self.gateway
                .create_session(
                    &principal,
                    &target,
                    overrides
                        .into_iter()
                        .filter_map(|(name, value)| value.map(|value| (name, value)))
                        .collect(),
                )
                .map_err(service_error_status)?
        };
        let mut response =
            action_result_response(SetSessionOptionsResult { errors }.encode_to_vec());
        self.set_session_cookie(&mut response, &principal, &snapshot)?;
        Ok(response)
    }

    fn get_session_options(
        &self,
        request: &Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        use session_proto::{GetSessionOptionsRequest, GetSessionOptionsResult};

        GetSessionOptionsRequest::decode(request.get_ref().body.as_ref())
            .map_err(|_| Status::invalid_argument("GetSessionOptions body is malformed"))?;
        let principal = required_principal(request)?.clone();
        let current = self
            .session_from_request(request, &principal)?
            .ok_or_else(|| Status::not_found("Flight session cookie is required"))?;
        let snapshot = self
            .gateway
            .session(&principal, &current.session_id)
            .map_err(service_error_status)?;
        let mut options = std::collections::HashMap::from([
            (
                "qcli.target".into(),
                string_session_option(snapshot.target.clone()),
            ),
            (
                "qcli.session_id".into(),
                string_session_option(snapshot.id.clone()),
            ),
            ("qcli.version".into(), int_session_option(snapshot.version)),
        ]);
        for (name, value) in &snapshot.overrides {
            options.insert(
                session_option_name(name),
                string_session_option(value.clone()),
            );
        }
        let mut response = action_result_response(
            GetSessionOptionsResult {
                session_options: options,
            }
            .encode_to_vec(),
        );
        self.set_session_cookie(&mut response, &principal, &snapshot)?;
        Ok(response)
    }

    fn close_session(
        &self,
        request: &Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        use session_proto::{CloseSessionRequest, CloseSessionResult, CloseSessionStatus};

        CloseSessionRequest::decode(request.get_ref().body.as_ref())
            .map_err(|_| Status::invalid_argument("CloseSession body is malformed"))?;
        let principal = required_principal(request)?.clone();
        let current = self
            .session_from_request(request, &principal)?
            .ok_or_else(|| Status::not_found("Flight session cookie is required"))?;
        self.gateway
            .close_session(&principal, &current.session_id)
            .map_err(service_error_status)?;
        let mut response = action_result_response(
            CloseSessionResult {
                status: CloseSessionStatus::Closed as i32,
            }
            .encode_to_vec(),
        );
        response.metadata_mut().insert(
            "set-cookie",
            MetadataValue::from_static(
                "arrow_flight_session_id=; Max-Age=0; Path=/; HttpOnly; SameSite=Strict",
            ),
        );
        Ok(response)
    }

    fn metadata_request<T>(
        &self,
        request: &Request<T>,
        principal: &AuthenticatedPrincipal,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> Result<MetadataRequest, Status> {
        if let Some(session) = self.session_from_request(request, principal)? {
            self.gateway
                .session_metadata_request(principal, &session.session_id, catalog, schema, None)
                .map_err(service_error_status)
        } else {
            let target = request
                .metadata()
                .get("qcli-target")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "qcli-target metadata or a Flight session cookie is required",
                    )
                })?;
            self.gateway
                .target_metadata_request(principal, target, catalog, schema, None)
                .map_err(service_error_status)
        }
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

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let principal = required_principal(&request)?.clone();
        let descriptor = request.get_ref().clone();
        let session = self.session_from_request(&request, &principal)?;
        let status = if let Some(session) = &session {
            self.gateway
                .submit_session_query(&principal, &session.session_id, query.query)
                .map_err(service_error_status)?
        } else {
            let target = request
                .metadata()
                .get("qcli-target")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "qcli-target metadata or a Flight session cookie is required",
                    )
                })?;
            self.gateway
                .submit_stateless_query(&principal, target, BTreeMap::new(), query.query)
                .map_err(service_error_status)?
        };
        let status = wait_for_schema(&self.gateway, &principal, &status.id).await?;
        let schema = self
            .gateway
            .query_schema(&principal, &status.id)
            .map_err(service_error_status)?;
        let signed = self
            .tickets
            .issue(&principal.id, &status.id, self.ticket_ttl)?;
        let ticket = TicketStatementQuery {
            statement_handle: signed.into(),
        };
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));
        let metadata = serde_json::to_vec(&serde_json::json!({
            "qcli_query_id": status.id,
            "engine_query_id": status.engine_query_id,
            "target": status.target,
        }))
        .map_err(|error| Status::internal(error.to_string()))?;
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|error| Status::internal(error.to_string()))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_total_records(if status.state == "completed" {
                i64::try_from(status.rows).unwrap_or(i64::MAX)
            } else {
                -1
            })
            .with_total_bytes(if status.state == "completed" {
                i64::try_from(status.retained_bytes).unwrap_or(i64::MAX)
            } else {
                -1
            })
            .with_ordered(true)
            .with_app_metadata(metadata);
        let mut response = Response::new(info);
        if let Some(session) = session {
            let snapshot = self
                .gateway
                .session(&principal, &session.session_id)
                .map_err(service_error_status)?;
            self.set_session_cookie(&mut response, &principal, &snapshot)?;
        }
        Ok(response)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        metadata_flight_info(&query, &schema, request.into_inner())
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        metadata_flight_info(&query, &schema, request.into_inner())
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        metadata_flight_info(&query, &schema, request.into_inner())
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        metadata_flight_info(&query, &schema, request.into_inner())
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder(&self.xdbc_info).schema();
        metadata_flight_info(&query, &schema, request.into_inner())
    }

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        metadata_flight_info(&query, &primary_key_schema(), request.into_inner())
    }

    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        metadata_flight_info(&query, &foreign_key_schema(), request.into_inner())
    }

    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        metadata_flight_info(&query, &foreign_key_schema(), request.into_inner())
    }

    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        metadata_flight_info(&query, &foreign_key_schema(), request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        let metadata_request = self.metadata_request(&request, &principal, None, None)?;
        let metadata = self.gateway.metadata();
        let capabilities = metadata
            .capabilities(&metadata_request.engine)
            .ok_or_else(|| Status::unimplemented("adapter capabilities are unavailable"))?;
        let identifiers = metadata
            .identifier_capabilities(&metadata_request.engine)
            .unwrap_or_default();
        let sql_info = sql_info_data(
            capabilities.supports(AdapterCapability::CancelQuery),
            &identifiers,
        );
        let builder = query.into_builder(&sql_info);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        let metadata_request = self.metadata_request(&request, &principal, None, None)?;
        let catalogs = self
            .gateway
            .metadata()
            .catalogs(metadata_request)
            .await
            .map_err(driver_status)?;
        let mut builder = query.into_builder();
        for catalog in catalogs {
            builder.append(catalog.name);
        }
        encode_metadata_batch(
            builder.build().map_err(Status::from)?,
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        let metadata_request =
            self.metadata_request(&request, &principal, query.catalog.clone(), None)?;
        let schemas = self
            .gateway
            .metadata()
            .schemas(metadata_request)
            .await
            .map_err(driver_status)?;
        let mut builder = query.into_builder();
        for schema in schemas {
            builder.append(schema.catalog.unwrap_or_default(), schema.name);
        }
        encode_metadata_batch(
            builder.build().map_err(Status::from)?,
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        let metadata_request =
            self.metadata_request(&request, &principal, query.catalog.clone(), None)?;
        let metadata = self.gateway.metadata();
        let objects = metadata
            .objects(metadata_request.clone())
            .await
            .map_err(driver_status)?;
        let mut builder = query.into_builder();
        for object in objects {
            let schema = if builder.schema().fields().len() == 5 {
                let mut describe = metadata_request.clone();
                describe.catalog.clone_from(&object.catalog);
                describe.schema.clone_from(&object.schema);
                let columns = metadata
                    .describe(describe, &object.name)
                    .await
                    .map_err(driver_status)?;
                columns_schema(
                    object.catalog.as_deref(),
                    object.schema.as_deref(),
                    &object.name,
                    &columns,
                )
            } else {
                Schema::empty()
            };
            builder
                .append(
                    object.catalog.unwrap_or_default(),
                    object.schema.unwrap_or_default(),
                    object.name,
                    table_type(object.kind),
                    &schema,
                )
                .map_err(Status::from)?;
        }
        encode_metadata_batch(
            builder.build().map_err(Status::from)?,
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        let metadata_request = self.metadata_request(&request, &principal, None, None)?;
        let objects = self
            .gateway
            .metadata()
            .objects(metadata_request)
            .await
            .map_err(driver_status)?;
        let mut types = objects
            .into_iter()
            .map(|object| table_type(object.kind))
            .collect::<std::collections::BTreeSet<_>>();
        if types.is_empty() {
            types.extend(["TABLE", "VIEW"]);
        }
        let mut builder = query.into_builder();
        for table_type in types {
            builder.append(table_type);
        }
        encode_metadata_batch(
            builder.build().map_err(Status::from)?,
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        self.metadata_request(&request, &principal, None, None)?;
        let batch = query
            .into_builder(&self.xdbc_info)
            .build()
            .map_err(Status::from)?;
        encode_metadata_batch(batch, self.max_flight_data_bytes)
    }

    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        self.metadata_request(&request, &principal, query.catalog, query.db_schema)?;
        encode_metadata_batch(
            empty_batch(primary_key_schema()),
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        self.metadata_request(&request, &principal, query.catalog, query.db_schema)?;
        encode_metadata_batch(
            empty_batch(foreign_key_schema()),
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        self.metadata_request(&request, &principal, query.catalog, query.db_schema)?;
        encode_metadata_batch(
            empty_batch(foreign_key_schema()),
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        self.metadata_request(&request, &principal, query.pk_catalog, query.pk_db_schema)?;
        encode_metadata_batch(
            empty_batch(foreign_key_schema()),
            self.max_flight_data_bytes,
        )
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let principal = required_principal(&request)?.clone();
        let payload = self
            .tickets
            .verify(&ticket.statement_handle, &principal.id)?;
        wait_for_terminal(&self.gateway, &principal, &payload.query_id).await?;
        let schema = self
            .gateway
            .query_schema(&principal, &payload.query_id)
            .map_err(service_error_status)?;
        let reader = self
            .gateway
            .result_reader(&principal, &payload.query_id)
            .map_err(service_error_status)?;
        let batches = stream::try_unfold(reader, next_result_batch);
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_max_flight_data_size(self.max_flight_data_bytes)
            .build(batches)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(encoded)))
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        let principal = required_principal(&request)?;
        let info = FlightInfo::decode(query.info)
            .map_err(|_| Status::invalid_argument("cancel request has invalid FlightInfo"))?;
        let ticket = info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.as_ref())
            .ok_or_else(|| Status::invalid_argument("cancel request has no query ticket"))?;
        let statement = decode_statement_ticket(&ticket.ticket)?;
        let payload = self
            .tickets
            .verify(&statement.statement_handle, &principal.id)?;
        self.gateway
            .cancel(principal, &payload.query_id)
            .map_err(service_error_status)?;
        Ok(ActionCancelQueryResult {
            // Flight SQL's protobuf value for CANCELLING. The generated enum is
            // not re-exported by arrow-flight, while the wire value is stable.
            result: 2,
        })
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        match request.get_ref().r#type.as_str() {
            SET_SESSION_OPTIONS => self.set_session_options(&request),
            GET_SESSION_OPTIONS => self.get_session_options(&request),
            CLOSE_SESSION => self.close_session(&request),
            action => Err(Status::invalid_argument(format!(
                "unsupported Flight action '{action}'"
            ))),
        }
    }

    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        Some(vec![
            Ok(ActionType {
                r#type: SET_SESSION_OPTIONS.into(),
                description: "Set or create the current Flight session".into(),
            }),
            Ok(ActionType {
                r#type: GET_SESSION_OPTIONS.into(),
                description: "Get current Flight session options".into(),
            }),
            Ok(ActionType {
                r#type: CLOSE_SESSION.into(),
                description: "Close the current Flight session".into(),
            }),
        ])
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
    let service = QcliFlightSql::with_limits(
        gateway,
        config.ticket_ttl,
        config.session_ttl,
        config.max_message_bytes.saturating_sub(1024),
    );
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

fn required_principal<T>(request: &Request<T>) -> Result<&AuthenticatedPrincipal, Status> {
    principal(request).ok_or_else(|| Status::unauthenticated("authenticated principal is missing"))
}

fn metadata_flight_info<M: ProstMessageExt>(
    query: &M,
    schema: &SchemaRef,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let ticket = Ticket::new(query.as_any().encode_to_vec());
    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(|error| Status::internal(error.to_string()))?
        .with_descriptor(descriptor)
        .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
        .with_total_records(-1)
        .with_total_bytes(-1)
        .with_ordered(true);
    Ok(Response::new(info))
}

#[allow(clippy::type_complexity, clippy::unnecessary_wraps)]
fn encode_metadata_batch(
    batch: RecordBatch,
    max_bytes: usize,
) -> Result<
    Response<Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>>,
    Status,
> {
    let schema = batch.schema();
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_max_flight_data_size(max_bytes)
        .build(stream::once(async { Ok(batch) }))
        .map_err(Status::from);
    Ok(Response::new(Box::pin(stream)))
}

fn driver_status(error: qcli_driver_api::DriverError) -> Status {
    if error.code == "unsupported_capability" {
        Status::unimplemented(error.message)
    } else {
        Status::unavailable(error.message)
    }
}

fn table_type(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Table => "TABLE",
        ObjectKind::View => "VIEW",
        ObjectKind::Other => "OTHER",
    }
}

fn columns_schema(
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
    columns: &[ColumnMetadata],
) -> Schema {
    let fields = columns.iter().map(|column| {
        let mut metadata = std::collections::HashMap::from([
            (
                "ARROW:FLIGHT:SQL:CATALOG_NAME".into(),
                catalog.unwrap_or_default().into(),
            ),
            (
                "ARROW:FLIGHT:SQL:DB_SCHEMA_NAME".into(),
                schema.unwrap_or_default().into(),
            ),
            ("ARROW:FLIGHT:SQL:TABLE_NAME".into(), table.into()),
            (
                "ARROW:FLIGHT:SQL:TYPE_NAME".into(),
                column.data_type.clone(),
            ),
        ]);
        if let Some(comment) = &column.comment {
            metadata.insert("ARROW:FLIGHT:SQL:REMARKS".into(), comment.clone());
        }
        Field::new(
            &column.name,
            arrow_type(&column.data_type),
            column.nullable.unwrap_or(true),
        )
        .with_metadata(metadata)
    });
    Schema::new(fields.collect::<Vec<_>>())
}

fn arrow_type(native: &str) -> DataType {
    let normalized = native.trim().to_ascii_lowercase();
    if let Some(inner) = type_arguments(&normalized, "array") {
        return DataType::List(Arc::new(Field::new_list_field(arrow_type(inner), true)));
    }
    if normalized == "array" {
        return DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)));
    }
    if let Some(inner) = type_arguments(&normalized, "map") {
        let parts = split_top_level(inner);
        if parts.len() == 2 {
            return DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", arrow_type(parts[0]), false)),
                            Arc::new(Field::new("value", arrow_type(parts[1]), true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
        }
    }
    if normalized == "object" || normalized == "map" {
        return DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
    }
    let base = normalized
        .split(['(', '<'])
        .next()
        .unwrap_or(&normalized)
        .trim();
    match base {
        "boolean" | "bool" => DataType::Boolean,
        "tinyint" | "byteint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "integer" | "int" => DataType::Int32,
        "bigint" | "long" => DataType::Int64,
        "real" | "float" => DataType::Float32,
        "double" | "double precision" => DataType::Float64,
        "binary" | "varbinary" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp" | "timestamp_ntz" | "timestamp_ltz" | "timestamp_tz" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "decimal" | "numeric" | "number" => {
            let (precision, scale) = decimal_shape(&normalized).unwrap_or((38, 9));
            DataType::Decimal128(precision, scale)
        }
        _ => DataType::Utf8,
    }
}

fn type_arguments<'a>(value: &'a str, name: &str) -> Option<&'a str> {
    let suffix = value.strip_prefix(name)?.trim();
    if (suffix.starts_with('(') && suffix.ends_with(')'))
        || (suffix.starts_with('<') && suffix.ends_with('>'))
    {
        Some(&suffix[1..suffix.len() - 1])
    } else {
        None
    }
}

fn split_top_level(value: &str) -> Vec<&str> {
    let mut depth = 0_i32;
    let mut start = 0;
    let mut output = Vec::new();
    for (index, character) in value.char_indices() {
        match character {
            '(' | '<' => depth += 1,
            ')' | '>' => depth -= 1,
            ',' if depth == 0 => {
                output.push(value[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    output.push(value[start..].trim());
    output
}

fn decimal_shape(value: &str) -> Option<(u8, i8)> {
    let values = value
        .split_once('(')?
        .1
        .trim_end_matches(')')
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    let precision = values.first()?.parse().ok()?;
    let scale = values.get(1).unwrap_or(&"0").parse().ok()?;
    Some((precision, scale))
}

fn primary_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_name", DataType::Utf8, true),
        Field::new("key_sequence", DataType::Int32, false),
    ]))
}

fn foreign_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk_catalog_name", DataType::Utf8, true),
        Field::new("pk_db_schema_name", DataType::Utf8, true),
        Field::new("pk_table_name", DataType::Utf8, false),
        Field::new("pk_column_name", DataType::Utf8, false),
        Field::new("fk_catalog_name", DataType::Utf8, true),
        Field::new("fk_db_schema_name", DataType::Utf8, true),
        Field::new("fk_table_name", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
        Field::new("fk_key_name", DataType::Utf8, true),
        Field::new("pk_key_name", DataType::Utf8, true),
        Field::new("update_rule", DataType::UInt8, false),
        Field::new("delete_rule", DataType::UInt8, false),
    ]))
}

fn empty_batch(schema: SchemaRef) -> RecordBatch {
    RecordBatch::new_empty(schema)
}

fn xdbc_type_info() -> XdbcTypeInfoData {
    let mut builder = XdbcTypeInfoDataBuilder::new();
    for (name, data_type, size, case_sensitive, radix) in [
        ("BOOLEAN", XdbcDataType::XdbcBit, Some(1), false, None),
        (
            "INTEGER",
            XdbcDataType::XdbcInteger,
            Some(32),
            false,
            Some(2),
        ),
        ("BIGINT", XdbcDataType::XdbcBigint, Some(64), false, Some(2)),
        ("DOUBLE", XdbcDataType::XdbcDouble, Some(53), false, Some(2)),
        (
            "DECIMAL",
            XdbcDataType::XdbcDecimal,
            Some(38),
            false,
            Some(10),
        ),
        ("VARCHAR", XdbcDataType::XdbcVarchar, None, true, None),
        ("VARBINARY", XdbcDataType::XdbcVarbinary, None, false, None),
        ("DATE", XdbcDataType::XdbcDate, Some(10), false, None),
        (
            "TIMESTAMP",
            XdbcDataType::XdbcTimestamp,
            Some(29),
            false,
            None,
        ),
    ] {
        builder.append(XdbcTypeInfo {
            type_name: name.into(),
            data_type,
            column_size: size,
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: data_type == XdbcDataType::XdbcDecimal,
            auto_increment: Some(false),
            local_type_name: Some(name.into()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: data_type,
            datetime_subcode: None,
            num_prec_radix: radix,
            interval_precision: None,
        });
    }
    builder.build().expect("static XDBC type metadata is valid")
}

fn sql_info_data(cancel: bool, identifiers: &IdentifierCapabilities) -> SqlInfoData {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "qcli");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::FlightSqlServerSubstrait, false);
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::None as i32,
    );
    builder.append(SqlInfo::FlightSqlServerCancel, cancel);
    builder.append(SqlInfo::FlightSqlServerBulkIngestion, false);
    builder.append(
        SqlInfo::SqlIdentifierCase,
        sql_identifier_case(identifiers.unquoted) as i32,
    );
    builder.append(SqlInfo::SqlIdentifierQuoteChar, identifiers.quote.as_str());
    builder.append(
        SqlInfo::SqlQuotedIdentifierCase,
        sql_identifier_case(identifiers.quoted) as i32,
    );
    builder.build().expect("static SQL info values are valid")
}

fn sql_identifier_case(value: IdentifierCase) -> SqlSupportedCaseSensitivity {
    match value {
        IdentifierCase::Insensitive => {
            SqlSupportedCaseSensitivity::SqlCaseSensitivityCaseInsensitive
        }
        IdentifierCase::Upper => SqlSupportedCaseSensitivity::SqlCaseSensitivityUppercase,
        IdentifierCase::Lower => SqlSupportedCaseSensitivity::SqlCaseSensitivityLowercase,
        IdentifierCase::Mixed => SqlSupportedCaseSensitivity::SqlCaseSensitivityUnknown,
    }
}

async fn wait_for_schema(
    gateway: &GatewayService,
    principal: &AuthenticatedPrincipal,
    query_id: &str,
) -> Result<qcli_service::QueryStatus, Status> {
    loop {
        match gateway.query_schema(principal, query_id) {
            Ok(_) => {
                return gateway
                    .query(principal, query_id)
                    .map_err(service_error_status);
            }
            Err(error)
                if error.kind == ServiceErrorKind::FailedPrecondition
                    && error.code == "query_running" =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(service_error_status(error)),
        }
    }
}

async fn wait_for_terminal(
    gateway: &GatewayService,
    principal: &AuthenticatedPrincipal,
    query_id: &str,
) -> Result<qcli_service::QueryStatus, Status> {
    loop {
        let status = gateway
            .query(principal, query_id)
            .map_err(service_error_status)?;
        if matches!(status.state.as_str(), "completed" | "cancelled" | "failed") {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Serialize, Deserialize)]
struct TicketPayload {
    version: u8,
    query_id: String,
    owner: String,
    expires_at: u64,
}

struct TicketSigner {
    key: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionTokenPayload {
    format_version: u8,
    session_version: u64,
    session_id: String,
    owner: String,
    expires_at: u64,
}

struct SessionSigner {
    key: [u8; 32],
}

impl SessionSigner {
    fn new() -> Self {
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        Self { key }
    }

    fn issue(
        &self,
        owner: &str,
        session_id: &str,
        version: u64,
        ttl: Duration,
    ) -> Result<Vec<u8>, Status> {
        let payload = SessionTokenPayload {
            format_version: 1,
            session_version: version,
            session_id: session_id.into(),
            owner: owner.into(),
            expires_at: now_unix().saturating_add(ttl.as_secs()),
        };
        sign_payload(&self.key, &payload)
    }

    fn verify(&self, token: &[u8], owner: &str) -> Result<SessionTokenPayload, Status> {
        let payload: SessionTokenPayload = verify_payload(&self.key, token, "session token")?;
        if payload.format_version != 1 {
            return Err(Status::invalid_argument(
                "Flight session token version is not supported",
            ));
        }
        if payload.owner != owner {
            return Err(Status::permission_denied(
                "Flight session belongs to another principal",
            ));
        }
        if payload.expires_at < now_unix() {
            return Err(Status::not_found("Flight session token has expired"));
        }
        Ok(payload)
    }
}

impl TicketSigner {
    fn new() -> Self {
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        Self { key }
    }

    fn issue(&self, owner: &str, query_id: &str, ttl: Duration) -> Result<Vec<u8>, Status> {
        let payload = TicketPayload {
            version: 1,
            query_id: query_id.into(),
            owner: owner.into(),
            expires_at: now_unix().saturating_add(ttl.as_secs()),
        };
        let payload =
            serde_json::to_vec(&payload).map_err(|error| Status::internal(error.to_string()))?;
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .map_err(|_| Status::internal("could not initialize ticket signer"))?;
        mac.update(&payload);
        let signature = mac.finalize().into_bytes();
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature)
        )
        .into_bytes())
    }

    fn verify(&self, ticket: &[u8], owner: &str) -> Result<TicketPayload, Status> {
        let ticket = std::str::from_utf8(ticket)
            .map_err(|_| Status::invalid_argument("query ticket is not valid UTF-8"))?;
        let (payload, signature) = ticket
            .split_once('.')
            .ok_or_else(|| Status::invalid_argument("query ticket has an invalid format"))?;
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| Status::invalid_argument("query ticket payload is invalid"))?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| Status::invalid_argument("query ticket signature is invalid"))?;
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .map_err(|_| Status::internal("could not initialize ticket verifier"))?;
        mac.update(&payload);
        mac.verify_slice(&signature)
            .map_err(|_| Status::permission_denied("query ticket signature is invalid"))?;
        let payload: TicketPayload = serde_json::from_slice(&payload)
            .map_err(|_| Status::invalid_argument("query ticket payload is invalid"))?;
        if payload.version != 1 {
            return Err(Status::invalid_argument(
                "query ticket version is not supported",
            ));
        }
        if payload.owner != owner {
            return Err(Status::permission_denied(
                "query ticket belongs to another principal",
            ));
        }
        if payload.expires_at < now_unix() {
            return Err(Status::not_found("query ticket has expired"));
        }
        Ok(payload)
    }
}

fn sign_payload<T: Serialize>(key: &[u8], payload: &T) -> Result<Vec<u8>, Status> {
    let payload =
        serde_json::to_vec(payload).map_err(|error| Status::internal(error.to_string()))?;
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| Status::internal("could not initialize token signer"))?;
    mac.update(&payload);
    let signature = mac.finalize().into_bytes();
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload),
        URL_SAFE_NO_PAD.encode(signature)
    )
    .into_bytes())
}

fn verify_payload<T: for<'de> Deserialize<'de>>(
    key: &[u8],
    token: &[u8],
    label: &str,
) -> Result<T, Status> {
    let token = std::str::from_utf8(token)
        .map_err(|_| Status::invalid_argument(format!("{label} is not valid UTF-8")))?;
    let (payload, signature) = token
        .split_once('.')
        .ok_or_else(|| Status::invalid_argument(format!("{label} has an invalid format")))?;
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| Status::invalid_argument(format!("{label} payload is invalid")))?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| Status::invalid_argument(format!("{label} signature is invalid")))?;
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| Status::internal("could not initialize token verifier"))?;
    mac.update(&payload);
    mac.verify_slice(&signature)
        .map_err(|_| Status::permission_denied(format!("{label} signature is invalid")))?;
    serde_json::from_slice(&payload)
        .map_err(|_| Status::invalid_argument(format!("{label} payload is invalid")))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn cookie<T>(request: &Request<T>, name: &str) -> Option<String> {
    request
        .metadata()
        .get_all("cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|entry| entry.trim().split_once('='))
        .find_map(|(key, value)| (key == name).then(|| value.to_owned()))
}

type ActionStream = Pin<Box<dyn Stream<Item = Result<FlightActionResult, Status>> + Send>>;

fn action_result_response(body: Vec<u8>) -> Response<ActionStream> {
    Response::new(Box::pin(stream::once(async move {
        Ok(FlightActionResult { body: body.into() })
    })))
}

enum ParsedSessionOption {
    Unset,
    Value(String),
}

fn session_option_string(value: session_proto::SessionOptionValue) -> Option<ParsedSessionOption> {
    use session_proto::session_option_value::OptionValue;
    Some(match value.option_value {
        None => ParsedSessionOption::Unset,
        Some(OptionValue::StringValue(value)) => ParsedSessionOption::Value(value),
        Some(OptionValue::BoolValue(value)) => ParsedSessionOption::Value(value.to_string()),
        Some(OptionValue::Int64Value(value)) => ParsedSessionOption::Value(value.to_string()),
        Some(OptionValue::DoubleValue(value)) if value.is_finite() => {
            ParsedSessionOption::Value(value.to_string())
        }
        Some(OptionValue::StringListValue(value)) => {
            ParsedSessionOption::Value(serde_json::to_string(&value.values).ok()?)
        }
        Some(OptionValue::DoubleValue(_)) => return None,
    })
}

fn session_property_name(option: &str) -> Option<String> {
    match option {
        "catalog" | "schema" | "timeout" => Some(option.into()),
        _ => option
            .strip_prefix("engine.")
            .filter(|name| !name.is_empty())
            .map(str::to_owned),
    }
}

fn session_option_name(property: &str) -> String {
    match property {
        "catalog" | "schema" | "timeout" => property.into(),
        _ => format!("engine.{property}"),
    }
}

fn string_session_option(value: String) -> session_proto::SessionOptionValue {
    session_proto::SessionOptionValue {
        option_value: Some(session_proto::session_option_value::OptionValue::StringValue(value)),
    }
}

fn int_session_option(value: u64) -> session_proto::SessionOptionValue {
    session_proto::SessionOptionValue {
        option_value: Some(
            session_proto::session_option_value::OptionValue::Int64Value(
                i64::try_from(value).unwrap_or(i64::MAX),
            ),
        ),
    }
}

async fn next_result_batch(
    mut reader: ResultBatchReader,
) -> Result<Option<(arrow_array::RecordBatch, ResultBatchReader)>, FlightError> {
    let batch = reader
        .next_batch()
        .map_err(|error| FlightError::Tonic(Box::new(service_error_status(error))))?;
    Ok(batch.map(|batch| (batch, reader)))
}

fn decode_statement_ticket(bytes: &[u8]) -> Result<TicketStatementQuery, Status> {
    let any = arrow_flight::sql::Any::decode(bytes)
        .map_err(|_| Status::invalid_argument("query ticket is malformed"))?;
    any.unpack()
        .map_err(|_| Status::invalid_argument("query ticket is malformed"))?
        .ok_or_else(|| Status::invalid_argument("ticket is not a statement-query ticket"))
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
            let id = match bearer {
                "valid-key" => "analyst",
                "other-key" => "other",
                _ => {
                    return Err(AuthenticationError {
                        kind: AuthenticationErrorKind::Invalid,
                        message: "invalid bearer credential".into(),
                    });
                }
            };
            Ok(AuthenticatedPrincipal {
                id: id.into(),
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
        client.set_header("qcli-target", "demo");
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
    async fn authentication_and_missing_target_fail_stably() {
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
                if status.code() == Code::InvalidArgument
        ));
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn statement_query_streams_and_ticket_can_be_replayed() {
        let (mut client, shutdown, task) = server(FlightServerConfig::default()).await;
        client.set_token("valid-key".into());
        client.set_header("qcli-target", "demo");
        let info = client
            .execute("select 1".into(), None)
            .await
            .expect("statement submission");
        assert_eq!(info.endpoint.len(), 1);
        assert!(info.total_records > 0);
        let ticket = info.endpoint[0].ticket.clone().expect("query ticket");

        for _ in 0..2 {
            let mut results = client.do_get(ticket.clone()).await.expect("DoGet");
            let mut rows = 0;
            while let Some(batch) = results.try_next().await.expect("Arrow batch") {
                assert_eq!(
                    batch.schema().as_ref(),
                    &info.clone().try_decode_schema().unwrap()
                );
                rows += batch.num_rows();
            }
            assert_eq!(i64::try_from(rows).unwrap(), info.total_records);
        }
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    async fn metadata_batches(
        client: &mut FlightSqlServiceClient<Channel>,
        info: &FlightInfo,
    ) -> Vec<RecordBatch> {
        let mut stream = client
            .do_get(info.endpoint[0].ticket.clone().unwrap())
            .await
            .unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            batches.push(batch);
        }
        batches
    }

    #[tokio::test]
    async fn target_aware_metadata_uses_exact_flight_sql_schemas() {
        let (mut client, shutdown, task) = server(FlightServerConfig::default()).await;
        client.set_token("valid-key".into());
        client.set_header("qcli-target", "demo");

        let catalogs = client.get_catalogs().await.unwrap();
        let batches = metadata_batches(&mut client, &catalogs).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            catalogs.clone().try_decode_schema().unwrap(),
            batches[0].schema().as_ref().clone()
        );

        let schemas = client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some("demo".into()),
                db_schema_filter_pattern: Some("pub%".into()),
            })
            .await
            .unwrap();
        assert_eq!(
            metadata_batches(&mut client, &schemas).await[0].num_rows(),
            1
        );

        let tables = client
            .get_tables(CommandGetTables {
                catalog: None,
                db_schema_filter_pattern: None,
                table_name_filter_pattern: Some("event%".into()),
                table_types: vec!["TABLE".into(), "VIEW".into()],
                include_schema: true,
            })
            .await
            .unwrap();
        let table_batches = metadata_batches(&mut client, &tables).await;
        assert_eq!(table_batches[0].num_rows(), 2);
        assert_eq!(table_batches[0].num_columns(), 5);

        let types = client.get_table_types().await.unwrap();
        assert_eq!(metadata_batches(&mut client, &types).await[0].num_rows(), 2);
        let xdbc = client
            .get_xdbc_type_info(CommandGetXdbcTypeInfo { data_type: None })
            .await
            .unwrap();
        assert!(metadata_batches(&mut client, &xdbc).await[0].num_rows() >= 9);

        let keys = client
            .get_primary_keys(CommandGetPrimaryKeys {
                catalog: None,
                db_schema: None,
                table: "events".into(),
            })
            .await
            .unwrap();
        let key_batches = metadata_batches(&mut client, &keys).await;
        assert_eq!(
            keys.clone().try_decode_schema().unwrap(),
            primary_key_schema().as_ref().clone()
        );
        assert!(key_batches.is_empty());

        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    async fn session_action(
        client: &mut FlightSqlServiceClient<Channel>,
        action_type: &str,
        body: Vec<u8>,
        bearer: &str,
        cookie: Option<&str>,
    ) -> Result<(Option<String>, Vec<u8>), Status> {
        let mut request = Request::new(Action {
            r#type: action_type.into(),
            body: body.into(),
        });
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_str(&format!("Bearer {bearer}")).unwrap(),
        );
        if let Some(cookie) = cookie {
            request.metadata_mut().insert(
                "cookie",
                MetadataValue::from_str(cookie).expect("valid cookie"),
            );
        }
        let response = client.inner_mut().do_action(request).await?;
        let set_cookie = response
            .metadata()
            .get("set-cookie")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut stream = response.into_inner();
        let body = stream
            .message()
            .await?
            .map_or_else(Vec::new, |result| result.body.to_vec());
        Ok((set_cookie, body))
    }

    fn cookie_header(set_cookie: &str) -> &str {
        set_cookie.split(';').next().unwrap()
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn standard_actions_create_mutate_use_get_and_close_session() {
        use session_proto::session_option_value::OptionValue;
        use session_proto::{
            CloseSessionRequest, CloseSessionResult, CloseSessionStatus, GetSessionOptionsRequest,
            GetSessionOptionsResult, SessionOptionValue, SetSessionOptionsRequest,
            SetSessionOptionsResult,
        };
        use std::collections::HashMap;

        let (mut client, shutdown, task) = server(FlightServerConfig::default()).await;
        let set = SetSessionOptionsRequest {
            session_options: HashMap::from([
                (
                    "qcli.target".into(),
                    SessionOptionValue {
                        option_value: Some(OptionValue::StringValue("demo".into())),
                    },
                ),
                (
                    "catalog".into(),
                    SessionOptionValue {
                        option_value: Some(OptionValue::StringValue("analytics".into())),
                    },
                ),
            ]),
        };
        let (set_cookie, body) = session_action(
            &mut client,
            SET_SESSION_OPTIONS,
            set.encode_to_vec(),
            "valid-key",
            None,
        )
        .await
        .unwrap();
        assert!(
            SetSessionOptionsResult::decode(body.as_slice())
                .unwrap()
                .errors
                .is_empty()
        );
        let set_cookie = set_cookie.unwrap();
        let cookie = cookie_header(&set_cookie);

        let mutate = SetSessionOptionsRequest {
            session_options: HashMap::from([
                (
                    "qcli.target".into(),
                    SessionOptionValue {
                        option_value: Some(OptionValue::StringValue("demo".into())),
                    },
                ),
                (
                    "schema".into(),
                    SessionOptionValue {
                        option_value: Some(OptionValue::StringValue("public".into())),
                    },
                ),
            ]),
        };
        let (mutated_cookie, _) = session_action(
            &mut client,
            SET_SESSION_OPTIONS,
            mutate.encode_to_vec(),
            "valid-key",
            Some(cookie),
        )
        .await
        .unwrap();
        let mutated_cookie = mutated_cookie.unwrap();
        let mutated_cookie = cookie_header(&mutated_cookie);
        let stale = session_action(
            &mut client,
            SET_SESSION_OPTIONS,
            SetSessionOptionsRequest {
                session_options: HashMap::new(),
            }
            .encode_to_vec(),
            "valid-key",
            Some(cookie),
        )
        .await
        .unwrap_err();
        assert_eq!(stale.code(), Code::Aborted);

        client.set_token("valid-key".into());
        client.set_header("cookie", mutated_cookie);
        let info = client.execute("select 1".into(), None).await.unwrap();
        let mut results = client
            .do_get(info.endpoint[0].ticket.clone().unwrap())
            .await
            .unwrap();
        assert!(results.try_next().await.unwrap().is_some());

        let (_, body) = session_action(
            &mut client,
            GET_SESSION_OPTIONS,
            GetSessionOptionsRequest {}.encode_to_vec(),
            "valid-key",
            Some(mutated_cookie),
        )
        .await
        .unwrap();
        let options = GetSessionOptionsResult::decode(body.as_slice())
            .unwrap()
            .session_options;
        assert_eq!(
            options["qcli.target"].option_value,
            Some(OptionValue::StringValue("demo".into()))
        );
        assert!(!options.contains_key("catalog"));
        assert_eq!(
            options["schema"].option_value,
            Some(OptionValue::StringValue("public".into()))
        );

        let error = session_action(
            &mut client,
            GET_SESSION_OPTIONS,
            GetSessionOptionsRequest {}.encode_to_vec(),
            "other-key",
            Some(mutated_cookie),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);

        let unauthorized = session_action(
            &mut client,
            SET_SESSION_OPTIONS,
            SetSessionOptionsRequest {
                session_options: HashMap::from([(
                    "qcli.target".into(),
                    SessionOptionValue {
                        option_value: Some(OptionValue::StringValue("secret".into())),
                    },
                )]),
            }
            .encode_to_vec(),
            "valid-key",
            Some(mutated_cookie),
        )
        .await
        .unwrap_err();
        assert_eq!(unauthorized.code(), Code::PermissionDenied);

        let (_, body) = session_action(
            &mut client,
            CLOSE_SESSION,
            CloseSessionRequest {}.encode_to_vec(),
            "valid-key",
            Some(mutated_cookie),
        )
        .await
        .unwrap();
        assert_eq!(
            CloseSessionResult::decode(body.as_slice()).unwrap().status,
            CloseSessionStatus::Closed as i32
        );
        let error = session_action(
            &mut client,
            GET_SESSION_OPTIONS,
            GetSessionOptionsRequest {}.encode_to_vec(),
            "valid-key",
            Some(mutated_cookie),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::NotFound);

        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[test]
    fn signed_tickets_are_opaque_tamper_evident_and_owner_bound() {
        let signer = TicketSigner::new();
        let ticket = signer
            .issue("analyst", "qcli_query_1", Duration::from_secs(60))
            .unwrap();
        assert!(!String::from_utf8_lossy(&ticket).contains("qcli_query_1"));
        assert_eq!(
            signer.verify(&ticket, "analyst").unwrap().query_id,
            "qcli_query_1"
        );
        assert_eq!(
            signer.verify(&ticket, "other").unwrap_err().code(),
            Code::PermissionDenied
        );
        let mut tampered = ticket;
        tampered[0] ^= 1;
        assert!(signer.verify(&tampered, "analyst").is_err());
    }

    #[test]
    fn session_tokens_are_versioned_expiring_and_owner_bound() {
        let signer = SessionSigner::new();
        let token = signer
            .issue("analyst", "session-1", 7, Duration::from_secs(60))
            .unwrap();
        let payload = signer.verify(&token, "analyst").unwrap();
        assert_eq!(payload.format_version, 1);
        assert_eq!(payload.session_version, 7);
        assert_eq!(
            signer.verify(&token, "other").unwrap_err().code(),
            Code::PermissionDenied
        );

        let expired = sign_payload(
            &signer.key,
            &SessionTokenPayload {
                format_version: 1,
                session_version: 7,
                session_id: "session-1".into(),
                owner: "analyst".into(),
                expires_at: 0,
            },
        )
        .unwrap();
        assert_eq!(
            signer.verify(&expired, "analyst").unwrap_err().code(),
            Code::NotFound
        );
        let serialized = String::from_utf8_lossy(&token);
        assert!(!serialized.contains("password"));
        assert!(!serialized.contains("select "));
        assert!(!serialized.contains("connection"));
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
