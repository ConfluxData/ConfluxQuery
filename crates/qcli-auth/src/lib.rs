//! Credential-provider contracts shared by engine adapters.

use async_trait::async_trait;
use std::fmt;

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

    #[test]
    fn secrets_are_redacted() {
        let credential = UsernamePasswordCredential::new("alice", "highly-secret");
        let debug = format!("{credential:?}");
        assert!(debug.contains("alice"));
        assert!(!debug.contains("highly-secret"));
    }
}
