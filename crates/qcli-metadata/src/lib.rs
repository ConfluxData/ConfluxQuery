//! Frontend-neutral metadata discovery with context-scoped caching.

use qcli_driver_api::{
    CatalogMetadata, ColumnMetadata, DriverError, EngineAdapter, MetadataRequest, ObjectMetadata,
    SchemaMetadata,
};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Operation {
    Catalogs,
    Schemas,
    Objects,
    Describe(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    target: String,
    engine: String,
    catalog: Option<String>,
    schema: Option<String>,
    pattern: Option<String>,
    properties: u64,
    operation: Operation,
}

#[derive(Clone)]
enum CachedValue {
    Catalogs(Vec<CatalogMetadata>),
    Schemas(Vec<SchemaMetadata>),
    Objects(Vec<ObjectMetadata>),
    Columns(Vec<ColumnMetadata>),
}

struct CacheEntry {
    inserted: Instant,
    value: CachedValue,
}

pub struct MetadataService {
    adapters: HashMap<String, Arc<dyn EngineAdapter>>,
    cache: Mutex<HashMap<CacheKey, CacheEntry>>,
    ttl: Duration,
}

impl MetadataService {
    #[must_use]
    pub fn new(adapters: impl IntoIterator<Item = Arc<dyn EngineAdapter>>, ttl: Duration) -> Self {
        Self {
            adapters: adapters
                .into_iter()
                .map(|adapter| (adapter.engine().to_owned(), adapter))
                .collect(),
            cache: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// List catalogs through the selected adapter.
    ///
    /// # Errors
    /// Returns a driver error for missing adapters or discovery failures.
    pub async fn catalogs(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<CatalogMetadata>, DriverError> {
        let key = cache_key(&request, Operation::Catalogs);
        if let Some(CachedValue::Catalogs(value)) = self.cached(&key) {
            return Ok(value);
        }
        let value = self
            .adapter(&request.engine)?
            .list_catalogs(request)
            .await?;
        self.insert(key, CachedValue::Catalogs(value.clone()));
        Ok(value)
    }

    /// List schemas through the selected adapter.
    ///
    /// # Errors
    /// Returns a driver error for missing adapters or discovery failures.
    pub async fn schemas(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<SchemaMetadata>, DriverError> {
        let key = cache_key(&request, Operation::Schemas);
        if let Some(CachedValue::Schemas(value)) = self.cached(&key) {
            return Ok(value);
        }
        let value = self.adapter(&request.engine)?.list_schemas(request).await?;
        self.insert(key, CachedValue::Schemas(value.clone()));
        Ok(value)
    }

    /// List tables and views through the selected adapter.
    ///
    /// # Errors
    /// Returns a driver error for missing adapters or discovery failures.
    pub async fn objects(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<ObjectMetadata>, DriverError> {
        let key = cache_key(&request, Operation::Objects);
        if let Some(CachedValue::Objects(value)) = self.cached(&key) {
            return Ok(value);
        }
        let value = self.adapter(&request.engine)?.list_objects(request).await?;
        self.insert(key, CachedValue::Objects(value.clone()));
        Ok(value)
    }

    /// Describe one table or view through the selected adapter.
    ///
    /// # Errors
    /// Returns a driver error for missing adapters or discovery failures.
    pub async fn describe(
        &self,
        request: MetadataRequest,
        object: &str,
    ) -> Result<Vec<ColumnMetadata>, DriverError> {
        let key = cache_key(&request, Operation::Describe(object.into()));
        if let Some(CachedValue::Columns(value)) = self.cached(&key) {
            return Ok(value);
        }
        let value = self
            .adapter(&request.engine)?
            .describe_object(request, object)
            .await?;
        self.insert(key, CachedValue::Columns(value.clone()));
        Ok(value)
    }

    /// Remove cached entries scoped to one target.
    ///
    /// # Panics
    /// Panics if another thread poisoned the metadata cache lock.
    pub fn invalidate_target(&self, target: &str) {
        self.cache
            .lock()
            .expect("metadata cache mutex poisoned")
            .retain(|key, _| key.target != target);
    }

    fn adapter(&self, engine: &str) -> Result<&Arc<dyn EngineAdapter>, DriverError> {
        self.adapters.get(engine).ok_or_else(|| {
            DriverError::new(
                "adapter_not_found",
                format!("no adapter registered for engine '{engine}'"),
            )
        })
    }

    fn cached(&self, key: &CacheKey) -> Option<CachedValue> {
        let mut cache = self.cache.lock().expect("metadata cache mutex poisoned");
        cache.retain(|_, entry| entry.inserted.elapsed() <= self.ttl);
        cache.get(key).map(|entry| entry.value.clone())
    }

    fn insert(&self, key: CacheKey, value: CachedValue) {
        self.cache
            .lock()
            .expect("metadata cache mutex poisoned")
            .insert(
                key,
                CacheEntry {
                    inserted: Instant::now(),
                    value,
                },
            );
    }
}

fn cache_key(request: &MetadataRequest, operation: Operation) -> CacheKey {
    let mut hasher = DefaultHasher::new();
    request.properties.hash(&mut hasher);
    CacheKey {
        target: request.target.clone(),
        engine: request.engine.clone(),
        catalog: request.catalog.clone(),
        schema: request.schema.clone(),
        pattern: request.pattern.clone(),
        properties: hasher.finish(),
        operation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use qcli_driver_api::{AdapterCapabilities, AdapterCapability, QueryRequest, QuerySink};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingAdapter(AtomicUsize);

    #[async_trait]
    impl EngineAdapter for CountingAdapter {
        fn engine(&self) -> &'static str {
            "counting"
        }
        fn capabilities(&self) -> AdapterCapabilities {
            AdapterCapabilities::from_supported([AdapterCapability::ListCatalogs])
        }
        async fn execute(
            &self,
            _request: QueryRequest,
            _sink: QuerySink,
        ) -> Result<(), DriverError> {
            Ok(())
        }
        async fn list_catalogs(
            &self,
            _request: MetadataRequest,
        ) -> Result<Vec<CatalogMetadata>, DriverError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(vec![CatalogMetadata {
                name: "catalog".into(),
            }])
        }
    }

    fn request(target: &str) -> MetadataRequest {
        MetadataRequest {
            target: target.into(),
            engine: "counting".into(),
            properties: BTreeMap::new(),
            catalog: None,
            schema: None,
            pattern: None,
        }
    }

    #[tokio::test]
    async fn cache_is_scoped_and_invalidated_by_target() {
        let adapter = Arc::new(CountingAdapter(AtomicUsize::new(0)));
        let service = MetadataService::new(
            vec![Arc::clone(&adapter) as Arc<dyn EngineAdapter>],
            Duration::from_secs(60),
        );
        service.catalogs(request("one")).await.unwrap();
        service.catalogs(request("one")).await.unwrap();
        assert_eq!(adapter.0.load(Ordering::Relaxed), 1);
        service.catalogs(request("two")).await.unwrap();
        assert_eq!(adapter.0.load(Ordering::Relaxed), 2);
        service.invalidate_target("one");
        service.catalogs(request("one")).await.unwrap();
        assert_eq!(adapter.0.load(Ordering::Relaxed), 3);
    }
}
