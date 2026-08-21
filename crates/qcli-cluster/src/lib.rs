//! Optional distributed coordination and shared-result boundaries for qcli.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

pub const STATE_SCHEMA_VERSION: i32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterError {
    pub code: &'static str,
    pub message: String,
}

impl ClusterError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for ClusterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ClusterError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRegistration {
    pub node_id: String,
    pub instance_version: String,
    pub capabilities: Vec<String>,
    pub draining: bool,
    pub lease_epoch: i64,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedResource {
    pub resource_id: String,
    pub principal_id: String,
    pub kind: String,
    pub version: i64,
    pub payload: Value,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryLease {
    pub query_id: String,
    pub principal_id: String,
    pub owner_node_id: String,
    pub fencing_token: i64,
    pub lease_expires_at: DateTime<Utc>,
}

#[async_trait]
pub trait ClusterStateStore: Send + Sync {
    async fn migrate(&self) -> Result<(), ClusterError>;
    async fn register_node(
        &self,
        node_id: &str,
        instance_version: &str,
        capabilities: &[String],
        ttl: Duration,
    ) -> Result<NodeRegistration, ClusterError>;
    async fn renew_node(
        &self,
        node_id: &str,
        ttl: Duration,
    ) -> Result<NodeRegistration, ClusterError>;
    async fn set_draining(&self, node_id: &str, draining: bool) -> Result<(), ClusterError>;
    async fn live_nodes(&self) -> Result<Vec<NodeRegistration>, ClusterError>;
    async fn put_resource(
        &self,
        resource: SharedResource,
        expected_version: Option<i64>,
    ) -> Result<SharedResource, ClusterError>;
    async fn get_resource(
        &self,
        kind: &str,
        resource_id: &str,
        principal_id: &str,
    ) -> Result<Option<SharedResource>, ClusterError>;
    async fn delete_resource(
        &self,
        kind: &str,
        resource_id: &str,
        principal_id: &str,
    ) -> Result<bool, ClusterError>;
    async fn claim_query(
        &self,
        query_id: &str,
        principal_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<QueryLease, ClusterError>;
    async fn renew_query(
        &self,
        lease: &QueryLease,
        ttl: Duration,
    ) -> Result<QueryLease, ClusterError>;
    async fn release_query(&self, lease: &QueryLease) -> Result<bool, ClusterError>;
    async fn acquire_quota(
        &self,
        principal_id: &str,
        quota: &str,
        limit: usize,
        ttl: Duration,
    ) -> Result<String, ClusterError>;
    async fn release_quota(&self, permit_id: &str) -> Result<(), ClusterError>;
}

#[async_trait]
pub trait ResultObjectStore: Send + Sync {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ClusterError>;
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ClusterError>;
    async fn delete(&self, key: &str) -> Result<(), ClusterError>;
}

#[derive(Clone)]
pub struct SharedObjectStore {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
}

impl SharedObjectStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into().trim_matches('/').to_owned(),
        }
    }

    /// Build a local-file or S3-compatible store from a URL and environment.
    ///
    /// # Errors
    /// Returns an error for an invalid URL or incomplete provider configuration.
    pub fn from_url(value: &str) -> Result<Self, ClusterError> {
        let url = url::Url::parse(value)
            .map_err(|error| ClusterError::new("object_store_url", error.to_string()))?;
        let (store, prefix) = object_store::parse_url(&url).map_err(object_error)?;
        Ok(Self::new(Arc::from(store), prefix.to_string()))
    }

    fn path(&self, key: &str) -> Result<ObjectPath, ClusterError> {
        if key.contains("..") || key.starts_with('/') {
            return Err(ClusterError::new(
                "invalid_object_key",
                "object key must be relative and cannot contain '..'",
            ));
        }
        ObjectPath::parse(format!("{}/{}", self.prefix, key.trim_start_matches('/')))
            .map_err(|error| ClusterError::new("invalid_object_key", error.to_string()))
    }
}

#[async_trait]
impl ResultObjectStore for SharedObjectStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ClusterError> {
        self.inner
            .put(&self.path(key)?, value.into())
            .await
            .map(|_| ())
            .map_err(object_error)
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ClusterError> {
        match self.inner.get(&self.path(key)?).await {
            Ok(result) => result.bytes().await.map(Some).map_err(object_error),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(object_error(error)),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ClusterError> {
        match self.inner.delete(&self.path(key)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(object_error(error)),
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err"
)]
fn object_error(error: object_store::Error) -> ClusterError {
    ClusterError::new("object_store", error.to_string())
}

#[derive(Default)]
pub struct MemoryClusterStateStore {
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    nodes: HashMap<String, NodeRegistration>,
    resources: HashMap<(String, String), SharedResource>,
    leases: HashMap<String, QueryLease>,
    permits: HashMap<String, (String, String, DateTime<Utc>)>,
    next_fence: i64,
}

#[async_trait]
impl ClusterStateStore for MemoryClusterStateStore {
    async fn migrate(&self) -> Result<(), ClusterError> {
        Ok(())
    }

    async fn register_node(
        &self,
        node_id: &str,
        instance_version: &str,
        capabilities: &[String],
        ttl: Duration,
    ) -> Result<NodeRegistration, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let epoch = state
            .nodes
            .get(node_id)
            .map_or(1, |node| node.lease_epoch + 1);
        let node = NodeRegistration {
            node_id: node_id.into(),
            instance_version: instance_version.into(),
            capabilities: capabilities.to_vec(),
            draining: false,
            lease_epoch: epoch,
            lease_expires_at: deadline(ttl),
        };
        state.nodes.insert(node_id.into(), node.clone());
        Ok(node)
    }

    async fn renew_node(
        &self,
        node_id: &str,
        ttl: Duration,
    ) -> Result<NodeRegistration, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let node = state.nodes.get_mut(node_id).ok_or_else(|| {
            ClusterError::new("node_not_found", "node registration does not exist")
        })?;
        node.lease_expires_at = deadline(ttl);
        Ok(node.clone())
    }

    async fn set_draining(&self, node_id: &str, draining: bool) -> Result<(), ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let node = state.nodes.get_mut(node_id).ok_or_else(|| {
            ClusterError::new("node_not_found", "node registration does not exist")
        })?;
        node.draining = draining;
        Ok(())
    }

    async fn live_nodes(&self) -> Result<Vec<NodeRegistration>, ClusterError> {
        let now = Utc::now();
        Ok(self
            .state
            .lock()
            .expect("memory cluster lock poisoned")
            .nodes
            .values()
            .filter(|node| node.lease_expires_at > now)
            .cloned()
            .collect())
    }

    async fn put_resource(
        &self,
        mut resource: SharedResource,
        expected_version: Option<i64>,
    ) -> Result<SharedResource, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let key = (resource.kind.clone(), resource.resource_id.clone());
        let current = state.resources.get(&key);
        if current.is_some_and(|value| value.principal_id != resource.principal_id) {
            return Err(ClusterError::new(
                "forbidden",
                "resource belongs to another principal",
            ));
        }
        let version = current.map_or(0, |value| value.version);
        if expected_version.is_some_and(|expected| expected != version) {
            return Err(ClusterError::new(
                "version_conflict",
                "resource version changed",
            ));
        }
        resource.version = version + 1;
        state.resources.insert(key, resource.clone());
        Ok(resource)
    }

    async fn get_resource(
        &self,
        kind: &str,
        resource_id: &str,
        principal_id: &str,
    ) -> Result<Option<SharedResource>, ClusterError> {
        let state = self.state.lock().expect("memory cluster lock poisoned");
        let Some(resource) = state.resources.get(&(kind.into(), resource_id.into())) else {
            return Ok(None);
        };
        if resource.principal_id != principal_id {
            return Err(ClusterError::new(
                "forbidden",
                "resource belongs to another principal",
            ));
        }
        Ok((resource.expires_at > Utc::now()).then(|| resource.clone()))
    }

    async fn delete_resource(
        &self,
        kind: &str,
        resource_id: &str,
        principal_id: &str,
    ) -> Result<bool, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let key = (kind.into(), resource_id.into());
        if state
            .resources
            .get(&key)
            .is_some_and(|resource| resource.principal_id != principal_id)
        {
            return Err(ClusterError::new(
                "forbidden",
                "resource belongs to another principal",
            ));
        }
        Ok(state.resources.remove(&key).is_some())
    }

    async fn claim_query(
        &self,
        query_id: &str,
        principal_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<QueryLease, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let now = Utc::now();
        if let Some(lease) = state.leases.get(query_id) {
            if lease.principal_id != principal_id {
                return Err(ClusterError::new(
                    "forbidden",
                    "query belongs to another principal",
                ));
            }
            if lease.lease_expires_at > now && lease.owner_node_id != node_id {
                return Err(ClusterError::new(
                    "lease_held",
                    "query lease is held by another live node",
                ));
            }
        }
        state.next_fence += 1;
        let lease = QueryLease {
            query_id: query_id.into(),
            principal_id: principal_id.into(),
            owner_node_id: node_id.into(),
            fencing_token: state.next_fence,
            lease_expires_at: deadline(ttl),
        };
        state.leases.insert(query_id.into(), lease.clone());
        Ok(lease)
    }

    async fn renew_query(
        &self,
        lease: &QueryLease,
        ttl: Duration,
    ) -> Result<QueryLease, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let current = state
            .leases
            .get_mut(&lease.query_id)
            .ok_or_else(|| ClusterError::new("lease_lost", "query lease does not exist"))?;
        if current.owner_node_id != lease.owner_node_id
            || current.fencing_token != lease.fencing_token
        {
            return Err(ClusterError::new(
                "lease_lost",
                "query fencing token is stale",
            ));
        }
        current.lease_expires_at = deadline(ttl);
        Ok(current.clone())
    }

    async fn release_query(&self, lease: &QueryLease) -> Result<bool, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        if state.leases.get(&lease.query_id).is_some_and(|current| {
            current.owner_node_id == lease.owner_node_id
                && current.fencing_token == lease.fencing_token
        }) {
            state.leases.remove(&lease.query_id);
            return Ok(true);
        }
        Ok(false)
    }

    async fn acquire_quota(
        &self,
        principal_id: &str,
        quota: &str,
        limit: usize,
        ttl: Duration,
    ) -> Result<String, ClusterError> {
        let mut state = self.state.lock().expect("memory cluster lock poisoned");
        let now = Utc::now();
        state.permits.retain(|_, (_, _, expires)| *expires > now);
        let used = state
            .permits
            .values()
            .filter(|(principal, name, _)| principal == principal_id && name == quota)
            .count();
        if used >= limit {
            return Err(ClusterError::new(
                "quota_exhausted",
                "distributed quota exhausted",
            ));
        }
        let id = Uuid::new_v4().to_string();
        state.permits.insert(
            id.clone(),
            (principal_id.into(), quota.into(), deadline(ttl)),
        );
        Ok(id)
    }

    async fn release_quota(&self, permit_id: &str) -> Result<(), ClusterError> {
        self.state
            .lock()
            .expect("memory cluster lock poisoned")
            .permits
            .remove(permit_id);
        Ok(())
    }
}

pub struct PostgresClusterStateStore {
    client: Client,
}

impl PostgresClusterStateStore {
    /// Connect to the `PostgreSQL` coordination store.
    ///
    /// # Errors
    /// Returns a connection error without exposing credentials.
    pub async fn connect(url: &str) -> Result<Self, ClusterError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(postgres_error)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self { client })
    }
}

#[async_trait]
impl ClusterStateStore for PostgresClusterStateStore {
    async fn migrate(&self) -> Result<(), ClusterError> {
        self.client
            .batch_execute(include_str!("schema.sql"))
            .await
            .map_err(postgres_error)?;
        let version: i32 = self
            .client
            .query_one(
                "SELECT version FROM qcli_cluster_schema WHERE singleton=true",
                &[],
            )
            .await
            .map_err(postgres_error)?
            .get(0);
        if version != STATE_SCHEMA_VERSION {
            return Err(ClusterError::new(
                "schema_version",
                format!(
                    "cluster schema version {version} is incompatible with supported version {STATE_SCHEMA_VERSION}"
                ),
            ));
        }
        Ok(())
    }

    async fn register_node(
        &self,
        node_id: &str,
        instance_version: &str,
        capabilities: &[String],
        ttl: Duration,
    ) -> Result<NodeRegistration, ClusterError> {
        let ttl_ms = millis(ttl)?;
        let capabilities = capabilities.to_vec();
        let row = self.client.query_one(
            "INSERT INTO qcli_nodes(node_id, instance_version, capabilities, draining, lease_epoch, lease_expires_at) VALUES($1,$2,$3,false,1,clock_timestamp()+$4::bigint*interval '1 millisecond') ON CONFLICT(node_id) DO UPDATE SET instance_version=EXCLUDED.instance_version, capabilities=EXCLUDED.capabilities, draining=false, lease_epoch=qcli_nodes.lease_epoch+1, lease_expires_at=clock_timestamp()+$4::bigint*interval '1 millisecond' RETURNING node_id,instance_version,capabilities,draining,lease_epoch,lease_expires_at",
            &[&node_id, &instance_version, &capabilities, &ttl_ms],
        ).await.map_err(postgres_error)?;
        Ok(node_from_row(&row))
    }

    async fn renew_node(
        &self,
        node_id: &str,
        ttl: Duration,
    ) -> Result<NodeRegistration, ClusterError> {
        let ttl_ms = millis(ttl)?;
        let row = self.client.query_opt("UPDATE qcli_nodes SET lease_expires_at=clock_timestamp()+$2::bigint*interval '1 millisecond' WHERE node_id=$1 RETURNING node_id,instance_version,capabilities,draining,lease_epoch,lease_expires_at", &[&node_id, &ttl_ms]).await.map_err(postgres_error)?
            .ok_or_else(|| ClusterError::new("node_not_found", "node registration does not exist"))?;
        Ok(node_from_row(&row))
    }

    async fn set_draining(&self, node_id: &str, draining: bool) -> Result<(), ClusterError> {
        let changed = self
            .client
            .execute(
                "UPDATE qcli_nodes SET draining=$2 WHERE node_id=$1",
                &[&node_id, &draining],
            )
            .await
            .map_err(postgres_error)?;
        if changed == 0 {
            return Err(ClusterError::new(
                "node_not_found",
                "node registration does not exist",
            ));
        }
        Ok(())
    }

    async fn live_nodes(&self) -> Result<Vec<NodeRegistration>, ClusterError> {
        self.client.query("SELECT node_id,instance_version,capabilities,draining,lease_epoch,lease_expires_at FROM qcli_nodes WHERE lease_expires_at > clock_timestamp() ORDER BY node_id", &[]).await.map_err(postgres_error).map(|rows| rows.iter().map(node_from_row).collect())
    }

    async fn put_resource(
        &self,
        resource: SharedResource,
        expected_version: Option<i64>,
    ) -> Result<SharedResource, ClusterError> {
        let row = self.client.query_opt(
            "INSERT INTO qcli_resources(kind,resource_id,principal_id,version,payload,expires_at) VALUES($1,$2,$3,1,$4,$5) ON CONFLICT(kind,resource_id) DO UPDATE SET version=qcli_resources.version+1,payload=EXCLUDED.payload,expires_at=EXCLUDED.expires_at WHERE qcli_resources.principal_id=EXCLUDED.principal_id AND ($6::bigint IS NULL OR qcli_resources.version=$6) RETURNING kind,resource_id,principal_id,version,payload,expires_at",
            &[&resource.kind,&resource.resource_id,&resource.principal_id,&resource.payload,&resource.expires_at,&expected_version],
        ).await.map_err(postgres_error)?.ok_or_else(|| ClusterError::new("version_conflict", "resource owner or version changed"))?;
        Ok(resource_from_row(&row))
    }

    async fn get_resource(
        &self,
        kind: &str,
        resource_id: &str,
        principal_id: &str,
    ) -> Result<Option<SharedResource>, ClusterError> {
        let row = self.client.query_opt("SELECT kind,resource_id,principal_id,version,payload,expires_at FROM qcli_resources WHERE kind=$1 AND resource_id=$2 AND expires_at>clock_timestamp()", &[&kind,&resource_id]).await.map_err(postgres_error)?;
        let Some(row) = row else { return Ok(None) };
        let resource = resource_from_row(&row);
        if resource.principal_id != principal_id {
            return Err(ClusterError::new(
                "forbidden",
                "resource belongs to another principal",
            ));
        }
        Ok(Some(resource))
    }

    async fn delete_resource(
        &self,
        kind: &str,
        resource_id: &str,
        principal_id: &str,
    ) -> Result<bool, ClusterError> {
        let changed = self
            .client
            .execute(
                "DELETE FROM qcli_resources WHERE kind=$1 AND resource_id=$2 AND principal_id=$3",
                &[&kind, &resource_id, &principal_id],
            )
            .await
            .map_err(postgres_error)?;
        Ok(changed > 0)
    }

    async fn claim_query(
        &self,
        query_id: &str,
        principal_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<QueryLease, ClusterError> {
        let ttl_ms = millis(ttl)?;
        let row = self.client.query_opt(
            "INSERT INTO qcli_query_leases(query_id,principal_id,owner_node_id,fencing_token,lease_expires_at) VALUES($1,$2,$3,nextval('qcli_fencing_token'),clock_timestamp()+$4::bigint*interval '1 millisecond') ON CONFLICT(query_id) DO UPDATE SET owner_node_id=EXCLUDED.owner_node_id,fencing_token=nextval('qcli_fencing_token'),lease_expires_at=EXCLUDED.lease_expires_at WHERE qcli_query_leases.principal_id=EXCLUDED.principal_id AND (qcli_query_leases.owner_node_id=EXCLUDED.owner_node_id OR qcli_query_leases.lease_expires_at<=clock_timestamp()) RETURNING query_id,principal_id,owner_node_id,fencing_token,lease_expires_at",
            &[&query_id,&principal_id,&node_id,&ttl_ms],
        ).await.map_err(postgres_error)?.ok_or_else(|| ClusterError::new("lease_held", "query lease is held by another live node"))?;
        Ok(lease_from_row(&row))
    }

    async fn renew_query(
        &self,
        lease: &QueryLease,
        ttl: Duration,
    ) -> Result<QueryLease, ClusterError> {
        let ttl_ms = millis(ttl)?;
        let row = self.client.query_opt("UPDATE qcli_query_leases SET lease_expires_at=clock_timestamp()+$4::bigint*interval '1 millisecond' WHERE query_id=$1 AND owner_node_id=$2 AND fencing_token=$3 RETURNING query_id,principal_id,owner_node_id,fencing_token,lease_expires_at", &[&lease.query_id,&lease.owner_node_id,&lease.fencing_token,&ttl_ms]).await.map_err(postgres_error)?.ok_or_else(|| ClusterError::new("lease_lost", "query fencing token is stale"))?;
        Ok(lease_from_row(&row))
    }

    async fn release_query(&self, lease: &QueryLease) -> Result<bool, ClusterError> {
        self.client.execute("DELETE FROM qcli_query_leases WHERE query_id=$1 AND owner_node_id=$2 AND fencing_token=$3", &[&lease.query_id,&lease.owner_node_id,&lease.fencing_token]).await.map_err(postgres_error).map(|changed| changed > 0)
    }

    async fn acquire_quota(
        &self,
        principal_id: &str,
        quota: &str,
        limit: usize,
        ttl: Duration,
    ) -> Result<String, ClusterError> {
        let ttl_ms = millis(ttl)?;
        let limit = i64::try_from(limit)
            .map_err(|_| ClusterError::new("invalid_quota", "quota limit is too large"))?;
        let permit = Uuid::new_v4().to_string();
        let row = self.client.query_opt("WITH cleaned AS (DELETE FROM qcli_quota_permits WHERE expires_at<=clock_timestamp()), locked AS (SELECT pg_advisory_xact_lock(hashtextextended($1||':'||$2,0))), available AS (SELECT count(*)<$3::bigint AS yes FROM qcli_quota_permits,locked WHERE principal_id=$1 AND quota=$2) INSERT INTO qcli_quota_permits(permit_id,principal_id,quota,expires_at) SELECT $4,$1,$2,clock_timestamp()+$5::bigint*interval '1 millisecond' FROM available WHERE yes RETURNING permit_id", &[&principal_id,&quota,&limit,&permit,&ttl_ms]).await.map_err(postgres_error)?;
        row.map(|row| row.get(0))
            .ok_or_else(|| ClusterError::new("quota_exhausted", "distributed quota exhausted"))
    }

    async fn release_quota(&self, permit_id: &str) -> Result<(), ClusterError> {
        self.client
            .execute(
                "DELETE FROM qcli_quota_permits WHERE permit_id=$1",
                &[&permit_id],
            )
            .await
            .map_err(postgres_error)?;
        Ok(())
    }
}

fn millis(duration: Duration) -> Result<i64, ClusterError> {
    i64::try_from(duration.as_millis())
        .map_err(|_| ClusterError::new("invalid_ttl", "lease TTL is too large"))
}

fn node_from_row(row: &tokio_postgres::Row) -> NodeRegistration {
    NodeRegistration {
        node_id: row.get(0),
        instance_version: row.get(1),
        capabilities: row.get(2),
        draining: row.get(3),
        lease_epoch: row.get(4),
        lease_expires_at: row.get(5),
    }
}

fn resource_from_row(row: &tokio_postgres::Row) -> SharedResource {
    SharedResource {
        kind: row.get(0),
        resource_id: row.get(1),
        principal_id: row.get(2),
        version: row.get(3),
        payload: row.get(4),
        expires_at: row.get(5),
    }
}

fn lease_from_row(row: &tokio_postgres::Row) -> QueryLease {
    QueryLease {
        query_id: row.get(0),
        principal_id: row.get(1),
        owner_node_id: row.get(2),
        fencing_token: row.get(3),
        lease_expires_at: row.get(4),
    }
}

fn deadline(ttl: Duration) -> DateTime<Utc> {
    Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::days(365))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err"
)]
fn postgres_error(error: tokio_postgres::Error) -> ClusterError {
    ClusterError::new("postgres", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn nodes_share_resources_without_crossing_principal_ownership() {
        let store = MemoryClusterStateStore::default();
        store
            .register_node("node-a", "1", &[], Duration::from_secs(30))
            .await
            .unwrap();
        store
            .register_node("node-b", "1", &[], Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(store.live_nodes().await.unwrap().len(), 2);
        store.set_draining("node-a", true).await.unwrap();
        let nodes = store.live_nodes().await.unwrap();
        assert!(
            nodes
                .iter()
                .any(|node| node.node_id == "node-a" && node.draining)
        );
        assert!(
            nodes
                .iter()
                .any(|node| node.node_id == "node-b" && !node.draining)
        );

        let resource = store
            .put_resource(
                SharedResource {
                    resource_id: "session-1".into(),
                    principal_id: "alice".into(),
                    kind: "session".into(),
                    version: 0,
                    payload: serde_json::json!({"target":"trino"}),
                    expires_at: deadline(Duration::from_secs(30)),
                },
                Some(0),
            )
            .await
            .unwrap();
        assert_eq!(resource.version, 1);
        assert!(
            store
                .get_resource("session", "session-1", "alice")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .get_resource("session", "session-1", "mallory")
                .await
                .unwrap_err()
                .code,
            "forbidden"
        );
    }

    #[tokio::test]
    async fn expired_query_lease_fails_over_with_a_new_fencing_token() {
        let store = MemoryClusterStateStore::default();
        let first = store
            .claim_query("query-1", "alice", "node-a", Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(
            store
                .claim_query("query-1", "alice", "node-b", Duration::from_secs(1))
                .await
                .unwrap_err()
                .code,
            "lease_held"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        let second = store
            .claim_query("query-1", "alice", "node-b", Duration::from_secs(1))
            .await
            .unwrap();
        assert!(second.fencing_token > first.fencing_token);
        assert_eq!(
            store
                .renew_query(&first, Duration::from_secs(1))
                .await
                .unwrap_err()
                .code,
            "lease_lost"
        );
    }

    #[tokio::test]
    async fn quotas_and_results_are_shared() {
        let state = MemoryClusterStateStore::default();
        let permit = state
            .acquire_quota("alice", "queries", 1, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(
            state
                .acquire_quota("alice", "queries", 1, Duration::from_secs(30))
                .await
                .unwrap_err()
                .code,
            "quota_exhausted"
        );
        state.release_quota(&permit).await.unwrap();
        state
            .acquire_quota("alice", "queries", 1, Duration::from_secs(30))
            .await
            .unwrap();

        let objects = SharedObjectStore::new(Arc::new(InMemory::new()), "qcli/v1");
        objects
            .put("alice/query-1.arrow", Bytes::from_static(b"arrow"))
            .await
            .unwrap();
        assert_eq!(
            objects.get("alice/query-1.arrow").await.unwrap(),
            Some(Bytes::from_static(b"arrow"))
        );
        objects.delete("alice/query-1.arrow").await.unwrap();
        assert!(objects.get("alice/query-1.arrow").await.unwrap().is_none());
    }

    #[tokio::test]
    #[ignore = "requires QCLI_TEST_POSTGRES_URL"]
    async fn postgres_coordination_profile() {
        let url = std::env::var("QCLI_TEST_POSTGRES_URL").expect("QCLI_TEST_POSTGRES_URL");
        let store = PostgresClusterStateStore::connect(&url).await.unwrap();
        store.migrate().await.unwrap();
        let suffix = Uuid::new_v4().to_string();
        let node_a = format!("node-a-{suffix}");
        let node_b = format!("node-b-{suffix}");
        let query = format!("query-{suffix}");
        let principal = format!("principal-{suffix}");
        store
            .register_node(&node_a, "test", &[], Duration::from_secs(30))
            .await
            .unwrap();
        store
            .register_node(&node_b, "test", &[], Duration::from_secs(30))
            .await
            .unwrap();
        let first = store
            .claim_query(&query, &principal, &node_a, Duration::from_millis(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = store
            .claim_query(&query, &principal, &node_b, Duration::from_secs(30))
            .await
            .unwrap();
        assert!(second.fencing_token > first.fencing_token);
        assert_eq!(
            store
                .renew_query(&first, Duration::from_secs(30))
                .await
                .unwrap_err()
                .code,
            "lease_lost"
        );
        let permit = store
            .acquire_quota(&principal, "queries", 1, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(
            store
                .acquire_quota(&principal, "queries", 1, Duration::from_secs(30))
                .await
                .unwrap_err()
                .code,
            "quota_exhausted"
        );
        store.release_quota(&permit).await.unwrap();
    }
}
