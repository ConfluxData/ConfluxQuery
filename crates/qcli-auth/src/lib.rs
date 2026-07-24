//! Credential-provider contracts shared by engine adapters.

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
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
    async fn authenticate(
        &self,
        bearer: &str,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError>;
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
    async fn authenticate(
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
}
