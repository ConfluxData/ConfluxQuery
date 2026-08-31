//! Credential-provider contracts shared by engine adapters.

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString(<redacted>)")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialError {
    pub message: String,
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CredentialError {}

#[async_trait]
pub trait BearerCredentialProvider: Send + Sync {
    fn method(&self) -> &'static str;
    async fn credential(&self) -> Result<SecretString, CredentialError>;
}

/// Identity and policy produced by an HTTP authentication provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    pub id: String,
    pub allowed_targets: BTreeSet<String>,
    pub max_sessions: usize,
    pub max_concurrent_queries: usize,
}

impl AuthenticatedPrincipal {
    #[must_use]
    pub fn can_use_target(&self, target: &str) -> bool {
        self.allowed_targets.contains("*") || self.allowed_targets.contains(target)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationErrorKind {
    Missing,
    Invalid,
    Configuration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationError {
    pub kind: AuthenticationErrorKind,
    pub message: String,
}

impl AuthenticationError {
    fn invalid() -> Self {
        Self {
            kind: AuthenticationErrorKind::Invalid,
            message: "invalid or expired bearer credential".into(),
        }
    }
}

impl fmt::Display for AuthenticationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AuthenticationError {}

/// Extensible boundary shared by opaque API keys and future JWT/OIDC providers.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Authenticate without external I/O for synchronous transport
    /// interceptors. Providers that require asynchronous discovery or token
    /// exchange may reject this path and override [`Self::authenticate`].
    ///
    /// # Errors
    ///
    /// Returns a classified missing, invalid, or provider-configuration error.
    fn authenticate_immediate(
        &self,
        _bearer: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        Err(AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: "authentication provider does not support immediate validation".into(),
        })
    }

    async fn authenticate(
        &self,
        bearer: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        self.authenticate_immediate(bearer)
    }
}

#[derive(Debug, Deserialize)]
struct AuthFile {
    principals: BTreeMap<String, PrincipalEntry>,
    keys: BTreeMap<String, KeyEntry>,
}

#[derive(Debug, Deserialize)]
struct PrincipalEntry {
    targets: BTreeSet<String>,
    #[serde(default = "default_max_sessions")]
    max_sessions: usize,
    #[serde(default = "default_max_concurrent_queries")]
    max_concurrent_queries: usize,
}

#[derive(Debug, Deserialize)]
struct KeyEntry {
    principal: String,
    secret_hash: String,
    #[serde(default = "enabled")]
    enabled: bool,
    expires_at: Option<DateTime<Utc>>,
}

const fn default_max_sessions() -> usize {
    8
}

const fn default_max_concurrent_queries() -> usize {
    4
}

const fn enabled() -> bool {
    true
}

#[derive(Debug)]
pub struct ApiKeyAuthenticator {
    path: PathBuf,
    principals: BTreeMap<String, PrincipalEntry>,
    keys: BTreeMap<String, KeyEntry>,
    dummy_hash: String,
}

impl ApiKeyAuthenticator {
    /// Load API-key hashes and principal policies from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable, insecure, malformed, or inconsistent
    /// authentication configuration.
    pub fn load(path: &Path) -> Result<Self, AuthenticationError> {
        validate_auth_permissions(path)?;
        let source = fs::read_to_string(path).map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!(
                "{}: cannot read authentication config: {error}",
                path.display()
            ),
        })?;
        let file: AuthFile = toml::from_str(&source).map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("{}: invalid authentication config: {error}", path.display()),
        })?;
        for (key_id, key) in &file.keys {
            if !file.principals.contains_key(&key.principal) {
                return Err(AuthenticationError {
                    kind: AuthenticationErrorKind::Configuration,
                    message: format!(
                        "{}: key '{key_id}' references missing principal '{}'",
                        path.display(),
                        key.principal
                    ),
                });
            }
            PasswordHash::new(&key.secret_hash).map_err(|_| AuthenticationError {
                kind: AuthenticationErrorKind::Configuration,
                message: format!("{}: key '{key_id}' has an invalid hash", path.display()),
            })?;
        }
        let (_, dummy_hash) = generate_api_key_material("dummy")?;
        Ok(Self {
            path: path.to_path_buf(),
            principals: file.principals,
            keys: file.keys,
            dummy_hash,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl Authenticator for ApiKeyAuthenticator {
    fn authenticate_immediate(
        &self,
        bearer: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        let (key_id, secret) = parse_api_key(bearer).ok_or_else(AuthenticationError::invalid)?;
        let key = self.keys.get(key_id);
        let hash = key.map_or(self.dummy_hash.as_str(), |entry| entry.secret_hash.as_str());
        let parsed_hash = PasswordHash::new(hash).map_err(|_| AuthenticationError::invalid())?;
        let verified = Argon2::default()
            .verify_password(secret.as_bytes(), &parsed_hash)
            .is_ok();
        let Some(key) = key else {
            return Err(AuthenticationError::invalid());
        };
        if !verified || !key.enabled || key.expires_at.is_some_and(|expiry| expiry <= Utc::now()) {
            return Err(AuthenticationError::invalid());
        }
        let policy = self
            .principals
            .get(&key.principal)
            .ok_or_else(AuthenticationError::invalid)?;
        Ok(AuthenticatedPrincipal {
            id: key.principal.clone(),
            allowed_targets: policy.targets.clone(),
            max_sessions: policy.max_sessions,
            max_concurrent_queries: policy.max_concurrent_queries,
        })
    }
}

#[derive(Debug, Deserialize)]
struct OidcFile {
    issuer: String,
    audiences: BTreeSet<String>,
    jwks_file: PathBuf,
    #[serde(default = "default_oidc_algorithms")]
    algorithms: BTreeSet<String>,
    #[serde(default = "default_groups_claim")]
    groups_claim: String,
    #[serde(default = "default_clock_skew")]
    clock_skew_seconds: u64,
    #[serde(default)]
    defaults: OidcPolicy,
    #[serde(default)]
    group_policies: BTreeMap<String, OidcPolicy>,
}

#[derive(Debug, Clone, Deserialize)]
struct OidcPolicy {
    #[serde(default)]
    targets: BTreeSet<String>,
    #[serde(default = "default_max_sessions")]
    max_sessions: usize,
    #[serde(default = "default_max_concurrent_queries")]
    max_concurrent_queries: usize,
}

impl Default for OidcPolicy {
    fn default() -> Self {
        Self {
            targets: BTreeSet::new(),
            max_sessions: default_max_sessions(),
            max_concurrent_queries: default_max_concurrent_queries(),
        }
    }
}

fn default_oidc_algorithms() -> BTreeSet<String> {
    ["RS256".into(), "ES256".into()].into_iter().collect()
}

fn default_groups_claim() -> String {
    "groups".into()
}

const fn default_clock_skew() -> u64 {
    30
}

struct JwksSnapshot {
    source: String,
    keys: JwkSet,
}

/// Hot-reloadable JWT validator for an OIDC issuer and its JWKS.
pub struct OidcAuthenticator {
    issuer: String,
    audiences: BTreeSet<String>,
    jwks_path: PathBuf,
    algorithms: Vec<Algorithm>,
    groups_claim: String,
    clock_skew_seconds: u64,
    defaults: OidcPolicy,
    group_policies: BTreeMap<String, OidcPolicy>,
    jwks: RwLock<JwksSnapshot>,
}

impl fmt::Debug for OidcAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcAuthenticator")
            .field("issuer", &self.issuer)
            .field("audiences", &self.audiences)
            .field("jwks_path", &self.jwks_path)
            .field("algorithms", &self.algorithms)
            .finish_non_exhaustive()
    }
}

impl OidcAuthenticator {
    /// Load issuer policy and the initial JWKS snapshot.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the policy or initial JWKS cannot be
    /// read or validated.
    pub fn load(path: &Path) -> Result<Self, AuthenticationError> {
        let source = fs::read_to_string(path).map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("{}: cannot read OIDC config: {error}", path.display()),
        })?;
        let mut file: OidcFile = toml::from_str(&source).map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("{}: invalid OIDC config: {error}", path.display()),
        })?;
        if file.issuer.trim().is_empty() || file.audiences.is_empty() {
            return Err(AuthenticationError {
                kind: AuthenticationErrorKind::Configuration,
                message: "OIDC issuer and at least one audience are required".into(),
            });
        }
        if file.jwks_file.is_relative() {
            file.jwks_file = path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(&file.jwks_file);
        }
        let algorithms = file
            .algorithms
            .iter()
            .map(|value| parse_algorithm(value))
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = load_jwks(&file.jwks_file)?;
        Ok(Self {
            issuer: file.issuer,
            audiences: file.audiences,
            jwks_path: file.jwks_file,
            algorithms,
            groups_claim: file.groups_claim,
            clock_skew_seconds: file.clock_skew_seconds,
            defaults: file.defaults,
            group_policies: file.group_policies,
            jwks: RwLock::new(snapshot),
        })
    }

    /// Reload changed key material. Invalid rotation fails closed and retains
    /// the last valid snapshot for requests using unchanged keys.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the JWKS cannot be read or parsed.
    ///
    /// # Panics
    ///
    /// Panics only if an earlier thread poisoned the internal JWKS lock.
    pub fn refresh(&self) -> Result<bool, AuthenticationError> {
        let source = fs::read_to_string(&self.jwks_path).map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("{}: cannot read JWKS: {error}", self.jwks_path.display()),
        })?;
        if self.jwks.read().expect("OIDC JWKS lock poisoned").source == source {
            return Ok(false);
        }
        let keys = serde_json::from_str(&source).map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("{}: invalid JWKS: {error}", self.jwks_path.display()),
        })?;
        *self.jwks.write().expect("OIDC JWKS lock poisoned") = JwksSnapshot { source, keys };
        Ok(true)
    }
}

impl Authenticator for OidcAuthenticator {
    fn authenticate_immediate(
        &self,
        bearer: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        self.refresh()?;
        let header = decode_header(bearer).map_err(|_| AuthenticationError::invalid())?;
        if !self.algorithms.contains(&header.alg) {
            return Err(AuthenticationError::invalid());
        }
        let kid = header
            .kid
            .as_deref()
            .ok_or_else(AuthenticationError::invalid)?;
        let snapshot = self.jwks.read().expect("OIDC JWKS lock poisoned");
        let jwk = snapshot
            .keys
            .find(kid)
            .ok_or_else(AuthenticationError::invalid)?;
        let key = DecodingKey::from_jwk(jwk).map_err(|_| AuthenticationError::invalid())?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(
            &self
                .audiences
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        );
        validation.leeway = self.clock_skew_seconds;
        validation.required_spec_claims = ["exp".into(), "sub".into()].into_iter().collect();
        let claims = decode::<Value>(bearer, &key, &validation)
            .map_err(|_| AuthenticationError::invalid())?
            .claims;
        let subject = claims
            .get("sub")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(AuthenticationError::invalid)?;
        let groups = claim_strings(claims.get(&self.groups_claim));
        let mut targets = self.defaults.targets.clone();
        let mut max_sessions = self.defaults.max_sessions;
        let mut max_concurrent_queries = self.defaults.max_concurrent_queries;
        for group in groups {
            if let Some(policy) = self.group_policies.get(&group) {
                targets.extend(policy.targets.iter().cloned());
                max_sessions = max_sessions.max(policy.max_sessions);
                max_concurrent_queries = max_concurrent_queries.max(policy.max_concurrent_queries);
            }
        }
        if targets.is_empty() {
            return Err(AuthenticationError::invalid());
        }
        Ok(AuthenticatedPrincipal {
            id: format!("{}#{subject}", self.issuer),
            allowed_targets: targets,
            max_sessions,
            max_concurrent_queries,
        })
    }
}

fn load_jwks(path: &Path) -> Result<JwksSnapshot, AuthenticationError> {
    let source = fs::read_to_string(path).map_err(|error| AuthenticationError {
        kind: AuthenticationErrorKind::Configuration,
        message: format!("{}: cannot read JWKS: {error}", path.display()),
    })?;
    let keys = serde_json::from_str(&source).map_err(|error| AuthenticationError {
        kind: AuthenticationErrorKind::Configuration,
        message: format!("{}: invalid JWKS: {error}", path.display()),
    })?;
    Ok(JwksSnapshot { source, keys })
}

fn parse_algorithm(value: &str) -> Result<Algorithm, AuthenticationError> {
    match value {
        "RS256" => Ok(Algorithm::RS256),
        "RS384" => Ok(Algorithm::RS384),
        "RS512" => Ok(Algorithm::RS512),
        "ES256" => Ok(Algorithm::ES256),
        "ES384" => Ok(Algorithm::ES384),
        "HS256" => Ok(Algorithm::HS256),
        _ => Err(AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("unsupported OIDC signing algorithm '{value}'"),
        }),
    }
}

fn claim_strings(value: Option<&Value>) -> BTreeSet<String> {
    match value {
        Some(Value::String(value)) => [value.clone()].into_iter().collect(),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => BTreeSet::new(),
    }
}

/// Select between multiple credential formats without changing transport code.
pub struct CompositeAuthenticator {
    providers: Vec<Arc<dyn Authenticator>>,
}

impl CompositeAuthenticator {
    #[must_use]
    pub fn new(providers: Vec<Arc<dyn Authenticator>>) -> Self {
        Self { providers }
    }
}

impl Authenticator for CompositeAuthenticator {
    fn authenticate_immediate(
        &self,
        bearer: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        for provider in &self.providers {
            match provider.authenticate_immediate(bearer) {
                Ok(principal) => return Ok(principal),
                Err(error) if error.kind == AuthenticationErrorKind::Invalid => {}
                Err(error) => return Err(error),
            }
        }
        Err(AuthenticationError::invalid())
    }
}

fn parse_api_key(key: &str) -> Option<(&str, &str)> {
    let rest = key.strip_prefix("qcli_k1_")?;
    let (key_id, secret) = rest.split_once('_')?;
    (!key_id.is_empty() && !secret.is_empty()).then_some((key_id, secret))
}

/// Generate a new opaque API key and its Argon2id hash.
///
/// The returned key is the only copy of the raw credential and should be shown
/// once. Only the returned hash belongs in configuration.
///
/// # Errors
///
/// Returns an error when the supplied key ID is invalid or hashing fails.
pub fn generate_api_key_material(
    key_id: &str,
) -> Result<(SecretString, String), AuthenticationError> {
    if key_id.is_empty()
        || !key_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err(AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: "key ID must contain only letters, digits, and '-'".into(),
        });
    }
    let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let salt =
        SaltString::encode_b64(Uuid::new_v4().as_bytes()).map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("could not generate API-key salt: {error}"),
        })?;
    let hash = Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("could not hash API key: {error}"),
        })?
        .to_string();
    Ok((
        SecretString::new(format!("qcli_k1_{key_id}_{secret}")),
        hash,
    ))
}

#[cfg(unix)]
fn validate_auth_permissions(path: &Path) -> Result<(), AuthenticationError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path)
        .map_err(|error| AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!("{}: cannot inspect permissions: {error}", path.display()),
        })?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(AuthenticationError {
            kind: AuthenticationErrorKind::Configuration,
            message: format!(
                "{}: authentication config permissions are {:o}; run: chmod 600 {}",
                path.display(),
                mode & 0o777,
                path.display()
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_auth_permissions(_path: &Path) -> Result<(), AuthenticationError> {
    Ok(())
}

#[derive(Clone)]
pub struct StaticBearerCredential {
    method: &'static str,
    token: SecretString,
}

impl StaticBearerCredential {
    #[must_use]
    pub fn new(method: &'static str, token: impl Into<String>) -> Self {
        Self {
            method,
            token: SecretString::new(token),
        }
    }
}

impl fmt::Debug for StaticBearerCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticBearerCredential")
            .field("method", &self.method)
            .field("token", &self.token)
            .finish()
    }
}

#[async_trait]
impl BearerCredentialProvider for StaticBearerCredential {
    fn method(&self) -> &'static str {
        self.method
    }

    async fn credential(&self) -> Result<SecretString, CredentialError> {
        Ok(self.token.clone())
    }
}

#[derive(Clone)]
pub struct UsernamePasswordCredential {
    username: String,
    password: SecretString,
}

impl UsernamePasswordCredential {
    #[must_use]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: SecretString::new(password),
        }
    }

    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    #[must_use]
    pub fn password(&self) -> &SecretString {
        &self.password
    }
}

impl fmt::Debug for UsernamePasswordCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UsernamePasswordCredential")
            .field("username", &self.username)
            .field("password", &self.password)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FILE: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn secrets_are_redacted() {
        let credential = UsernamePasswordCredential::new("alice", "highly-secret");
        let debug = format!("{credential:?}");
        assert!(debug.contains("alice"));
        assert!(!debug.contains("highly-secret"));
    }

    #[tokio::test]
    async fn opaque_key_resolves_to_principal_without_storing_raw_secret() {
        let (key, hash) = generate_api_key_material("analytics").unwrap();
        let id = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("qcli-auth-{}-{id}.toml", std::process::id()));
        fs::write(
            &path,
            format!(
                "[principals.analytics]\ntargets=[\"trino\"]\nmax_sessions=2\nmax_concurrent_queries=3\n\n[keys.analytics]\nprincipal=\"analytics\"\nsecret_hash={hash:?}\n"
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let authenticator = ApiKeyAuthenticator::load(&path).unwrap();
        let principal = authenticator.authenticate(key.expose()).await.unwrap();
        assert_eq!(principal.id, "analytics");
        assert!(principal.can_use_target("trino"));
        assert!(!principal.can_use_target("snowflake"));
        assert_eq!(principal.max_sessions, 2);
        assert_eq!(principal.max_concurrent_queries, 3);
        assert!(
            authenticator
                .authenticate("qcli_k1_unknown_bad")
                .await
                .is_err()
        );
        assert!(!fs::read_to_string(&path).unwrap().contains(key.expose()));
        fs::remove_file(path).ok();
    }

    #[test]
    fn oidc_validates_claims_maps_groups_and_hot_reloads_jwks() {
        let id = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!("qcli-oidc-{}-{id}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let config_path = directory.join("oidc.toml");
        let jwks_path = directory.join("jwks.json");
        fs::write(
            &config_path,
            r#"issuer="https://idp.example"
audiences=["qcli"]
jwks_file="jwks.json"
algorithms=["HS256"]

[defaults]
targets=[]
max_sessions=1
max_concurrent_queries=1

[group_policies.analytics]
targets=["trino", "databricks"]
max_sessions=4
max_concurrent_queries=3
"#,
        )
        .unwrap();
        write_oct_jwks(&jwks_path, "first", "c2VjcmV0LW9uZQ");
        let authenticator = OidcAuthenticator::load(&config_path).unwrap();
        let first = oidc_token("first", b"secret-one", "qcli", "alice");
        let principal = authenticator.authenticate_immediate(&first).unwrap();
        assert_eq!(principal.id, "https://idp.example#alice");
        assert!(principal.can_use_target("trino"));
        assert!(!principal.can_use_target("snowflake"));
        assert_eq!(principal.max_sessions, 4);

        write_oct_jwks(&jwks_path, "second", "c2VjcmV0LXR3bw");
        let second = oidc_token("second", b"secret-two", "qcli", "alice");
        assert!(authenticator.authenticate_immediate(&first).is_err());
        assert!(authenticator.authenticate_immediate(&second).is_ok());
        let wrong_audience = oidc_token("second", b"secret-two", "warehouse", "alice");
        assert!(
            authenticator
                .authenticate_immediate(&wrong_audience)
                .is_err()
        );
        fs::remove_dir_all(directory).ok();
    }

    fn write_oct_jwks(path: &Path, kid: &str, key: &str) {
        fs::write(
            path,
            serde_json::to_vec(&json!({
                "keys": [{"kty": "oct", "kid": kid, "alg": "HS256", "k": key}]
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn oidc_token(kid: &str, secret: &[u8], audience: &str, subject: &str) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.into());
        encode(
            &header,
            &json!({
                "iss": "https://idp.example",
                "aud": audience,
                "sub": subject,
                "exp": Utc::now().timestamp() + 300,
                "groups": ["analytics"]
            }),
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }
}
