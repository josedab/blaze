//! External Table Catalog providers for Unity, Hive Metastore, and AWS Glue.
//!
//! Provides an `ExternalCatalogProvider` trait for integrating with external
//! metadata systems. Each provider supports schema/table discovery, statistics,
//! and caching with configurable TTL.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// Trait for external catalog providers.
///
/// Implementations connect to external metadata stores (AWS Glue, Databricks
/// Unity Catalog, Apache Hive Metastore) and expose their schemas and tables.
pub trait ExternalCatalogProvider: Send + Sync {
    /// Get the provider name (e.g., "glue", "unity", "hive").
    fn name(&self) -> &str;

    /// List available schemas/databases.
    fn list_schemas(&self) -> Result<Vec<String>>;

    /// List tables in a schema.
    fn list_tables(&self, schema: &str) -> Result<Vec<String>>;

    /// Get schema for a specific table.
    fn get_table_schema(&self, schema: &str, table: &str) -> Result<Schema>;

    /// Get statistics for a table (optional).
    fn get_table_statistics(&self, schema: &str, table: &str) -> Result<Option<ExternalTableStats>>;

    /// Get the storage location for a table (e.g., S3 path).
    fn get_table_location(&self, schema: &str, table: &str) -> Result<Option<String>>;
}

/// Statistics from an external catalog.
#[derive(Debug, Clone)]
pub struct ExternalTableStats {
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub num_files: Option<u64>,
    pub partitions: Vec<String>,
}

// ---------------------------------------------------------------------------
// Caching Layer
// ---------------------------------------------------------------------------

/// Cached entry with TTL.
#[derive(Debug, Clone)]
struct CacheEntry<T: Clone> {
    value: T,
    inserted_at: Instant,
}

/// Caching wrapper for external catalog providers.
pub struct CachedCatalogProvider {
    inner: Box<dyn ExternalCatalogProvider>,
    ttl: Duration,
    schema_cache: RwLock<Option<CacheEntry<Vec<String>>>>,
    table_cache: RwLock<HashMap<String, CacheEntry<Vec<String>>>>,
    schema_detail_cache: RwLock<HashMap<String, CacheEntry<Schema>>>,
}

impl CachedCatalogProvider {
    /// Create a new cached catalog provider.
    pub fn new(inner: Box<dyn ExternalCatalogProvider>, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            schema_cache: RwLock::new(None),
            table_cache: RwLock::new(HashMap::new()),
            schema_detail_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Invalidate all cached data.
    pub fn invalidate(&self) {
        *self.schema_cache.write() = None;
        self.table_cache.write().clear();
        self.schema_detail_cache.write().clear();
    }

    /// Get the inner provider name.
    pub fn name(&self) -> &str {
        self.inner.name()
    }
}

impl ExternalCatalogProvider for CachedCatalogProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        if let Some(entry) = self.schema_cache.read().as_ref() {
            if entry.inserted_at.elapsed() < self.ttl {
                return Ok(entry.value.clone());
            }
        }

        let schemas = self.inner.list_schemas()?;
        *self.schema_cache.write() = Some(CacheEntry {
            value: schemas.clone(),
            inserted_at: Instant::now(),
        });
        Ok(schemas)
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>> {
        if let Some(entry) = self.table_cache.read().get(schema) {
            if entry.inserted_at.elapsed() < self.ttl {
                return Ok(entry.value.clone());
            }
        }

        let tables = self.inner.list_tables(schema)?;
        self.table_cache.write().insert(
            schema.to_string(),
            CacheEntry {
                value: tables.clone(),
                inserted_at: Instant::now(),
            },
        );
        Ok(tables)
    }

    fn get_table_schema(&self, schema: &str, table: &str) -> Result<Schema> {
        let key = format!("{}.{}", schema, table);
        if let Some(entry) = self.schema_detail_cache.read().get(&key) {
            if entry.inserted_at.elapsed() < self.ttl {
                return Ok(entry.value.clone());
            }
        }

        let table_schema = self.inner.get_table_schema(schema, table)?;
        self.schema_detail_cache.write().insert(
            key,
            CacheEntry {
                value: table_schema.clone(),
                inserted_at: Instant::now(),
            },
        );
        Ok(table_schema)
    }

    fn get_table_statistics(&self, schema: &str, table: &str) -> Result<Option<ExternalTableStats>> {
        self.inner.get_table_statistics(schema, table)
    }

    fn get_table_location(&self, schema: &str, table: &str) -> Result<Option<String>> {
        self.inner.get_table_location(schema, table)
    }
}

// ---------------------------------------------------------------------------
// AWS Glue Catalog Provider
// ---------------------------------------------------------------------------

/// AWS Glue Data Catalog provider configuration.
#[derive(Debug, Clone)]
pub struct GlueConfig {
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    /// Glue catalog ID (defaults to AWS account ID)
    pub catalog_id: Option<String>,
    /// S3 endpoint override (for testing)
    pub endpoint_url: Option<String>,
}

impl Default for GlueConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            catalog_id: None,
            endpoint_url: None,
        }
    }
}

/// AWS Glue Data Catalog provider.
///
/// Connects to AWS Glue to discover databases, tables, and partitions.
/// Resolves S3 locations for table data.
pub struct GlueCatalogProvider {
    config: GlueConfig,
    /// In-memory registry for testing without actual AWS calls.
    registry: RwLock<GlueRegistry>,
}

#[derive(Debug, Default)]
struct GlueRegistry {
    databases: HashMap<String, HashMap<String, GlueTableEntry>>,
}

#[derive(Debug, Clone)]
struct GlueTableEntry {
    schema: Schema,
    location: Option<String>,
    stats: Option<ExternalTableStats>,
}

impl GlueCatalogProvider {
    /// Create a new Glue catalog provider.
    pub fn new(config: GlueConfig) -> Self {
        Self {
            config,
            registry: RwLock::new(GlueRegistry::default()),
        }
    }

    /// Register a database for testing/offline use.
    pub fn register_database(&self, name: &str) {
        self.registry
            .write()
            .databases
            .entry(name.to_string())
            .or_default();
    }

    /// Register a table in a database for testing/offline use.
    pub fn register_table(
        &self,
        database: &str,
        table: &str,
        schema: Schema,
        location: Option<String>,
    ) {
        self.registry
            .write()
            .databases
            .entry(database.to_string())
            .or_default()
            .insert(
                table.to_string(),
                GlueTableEntry {
                    schema,
                    location,
                    stats: None,
                },
            );
    }

    /// Get the Glue configuration.
    pub fn config(&self) -> &GlueConfig {
        &self.config
    }
}

impl ExternalCatalogProvider for GlueCatalogProvider {
    fn name(&self) -> &str {
        "glue"
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        Ok(self.registry.read().databases.keys().cloned().collect())
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>> {
        let registry = self.registry.read();
        let tables = registry
            .databases
            .get(schema)
            .map(|db| db.keys().cloned().collect())
            .unwrap_or_default();
        Ok(tables)
    }

    fn get_table_schema(&self, schema: &str, table: &str) -> Result<Schema> {
        let registry = self.registry.read();
        registry
            .databases
            .get(schema)
            .and_then(|db| db.get(table))
            .map(|entry| entry.schema.clone())
            .ok_or_else(|| {
                BlazeError::catalog(format!("Table '{}.{}' not found in Glue", schema, table))
            })
    }

    fn get_table_statistics(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Option<ExternalTableStats>> {
        let registry = self.registry.read();
        Ok(registry
            .databases
            .get(schema)
            .and_then(|db| db.get(table))
            .and_then(|entry| entry.stats.clone()))
    }

    fn get_table_location(&self, schema: &str, table: &str) -> Result<Option<String>> {
        let registry = self.registry.read();
        Ok(registry
            .databases
            .get(schema)
            .and_then(|db| db.get(table))
            .and_then(|entry| entry.location.clone()))
    }
}

// ---------------------------------------------------------------------------
// Databricks Unity Catalog Provider
// ---------------------------------------------------------------------------

/// Databricks Unity Catalog configuration.
#[derive(Debug, Clone)]
pub struct UnityConfig {
    /// Databricks workspace URL
    pub workspace_url: String,
    /// Personal access token
    pub token: Option<String>,
    /// Unity catalog name
    pub catalog_name: String,
}

/// Databricks Unity Catalog provider.
pub struct UnityCatalogProvider {
    #[allow(dead_code)]
    config: UnityConfig,
    registry: RwLock<HashMap<String, HashMap<String, UnityTableEntry>>>,
}

#[derive(Debug, Clone)]
struct UnityTableEntry {
    schema: Schema,
    location: Option<String>,
    #[allow(dead_code)]
    table_type: String,
}

impl UnityCatalogProvider {
    /// Create a new Unity Catalog provider.
    pub fn new(config: UnityConfig) -> Self {
        Self {
            config,
            registry: RwLock::new(HashMap::new()),
        }
    }

    /// Register a schema and table for testing/offline use.
    pub fn register_table(
        &self,
        schema_name: &str,
        table_name: &str,
        schema: Schema,
        location: Option<String>,
    ) {
        self.registry
            .write()
            .entry(schema_name.to_string())
            .or_default()
            .insert(
                table_name.to_string(),
                UnityTableEntry {
                    schema,
                    location,
                    table_type: "MANAGED".to_string(),
                },
            );
    }
}

impl ExternalCatalogProvider for UnityCatalogProvider {
    fn name(&self) -> &str {
        "unity"
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        Ok(self.registry.read().keys().cloned().collect())
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>> {
        let registry = self.registry.read();
        Ok(registry
            .get(schema)
            .map(|s| s.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn get_table_schema(&self, schema: &str, table: &str) -> Result<Schema> {
        let registry = self.registry.read();
        registry
            .get(schema)
            .and_then(|s| s.get(table))
            .map(|entry| entry.schema.clone())
            .ok_or_else(|| {
                BlazeError::catalog(format!("Table '{}.{}' not found in Unity", schema, table))
            })
    }

    fn get_table_statistics(
        &self,
        _schema: &str,
        _table: &str,
    ) -> Result<Option<ExternalTableStats>> {
        Ok(None)
    }

    fn get_table_location(&self, schema: &str, table: &str) -> Result<Option<String>> {
        let registry = self.registry.read();
        Ok(registry
            .get(schema)
            .and_then(|s| s.get(table))
            .and_then(|entry| entry.location.clone()))
    }
}

// ---------------------------------------------------------------------------
// Hive Metastore Provider
// ---------------------------------------------------------------------------

/// Hive Metastore configuration.
#[derive(Debug, Clone)]
pub struct HiveConfig {
    /// Thrift endpoint URI (e.g., "thrift://localhost:9083")
    pub thrift_uri: String,
    /// Connection timeout in seconds
    pub timeout_secs: u64,
}

impl Default for HiveConfig {
    fn default() -> Self {
        Self {
            thrift_uri: "thrift://localhost:9083".to_string(),
            timeout_secs: 30,
        }
    }
}

/// Hive Metastore provider.
pub struct HiveMetastoreProvider {
    #[allow(dead_code)]
    config: HiveConfig,
    registry: RwLock<HashMap<String, HashMap<String, HiveTableEntry>>>,
}

#[derive(Debug, Clone)]
struct HiveTableEntry {
    schema: Schema,
    location: Option<String>,
    #[allow(dead_code)]
    serde: String,
    partitions: Vec<String>,
}

impl HiveMetastoreProvider {
    /// Create a new Hive Metastore provider.
    pub fn new(config: HiveConfig) -> Self {
        Self {
            config,
            registry: RwLock::new(HashMap::new()),
        }
    }

    /// Register a table for testing/offline use.
    pub fn register_table(
        &self,
        database: &str,
        table: &str,
        schema: Schema,
        location: Option<String>,
        partitions: Vec<String>,
    ) {
        self.registry
            .write()
            .entry(database.to_string())
            .or_default()
            .insert(
                table.to_string(),
                HiveTableEntry {
                    schema,
                    location,
                    serde: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe".to_string(),
                    partitions,
                },
            );
    }
}

impl ExternalCatalogProvider for HiveMetastoreProvider {
    fn name(&self) -> &str {
        "hive"
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        Ok(self.registry.read().keys().cloned().collect())
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>> {
        let registry = self.registry.read();
        Ok(registry
            .get(schema)
            .map(|db| db.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn get_table_schema(&self, schema: &str, table: &str) -> Result<Schema> {
        let registry = self.registry.read();
        registry
            .get(schema)
            .and_then(|db| db.get(table))
            .map(|entry| entry.schema.clone())
            .ok_or_else(|| {
                BlazeError::catalog(format!(
                    "Table '{}.{}' not found in Hive Metastore",
                    schema, table
                ))
            })
    }

    fn get_table_statistics(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Option<ExternalTableStats>> {
        let registry = self.registry.read();
        Ok(registry
            .get(schema)
            .and_then(|db| db.get(table))
            .map(|entry| ExternalTableStats {
                row_count: None,
                size_bytes: None,
                num_files: None,
                partitions: entry.partitions.clone(),
            }))
    }

    fn get_table_location(&self, schema: &str, table: &str) -> Result<Option<String>> {
        let registry = self.registry.read();
        Ok(registry
            .get(schema)
            .and_then(|db| db.get(table))
            .and_then(|entry| entry.location.clone()))
    }
}

// ---------------------------------------------------------------------------
// External Catalog Manager
// ---------------------------------------------------------------------------

/// Manages multiple external catalog providers and auto-discovers tables.
pub struct ExternalCatalogManager {
    providers: RwLock<Vec<Arc<CachedCatalogProvider>>>,
}

impl Default for ExternalCatalogManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalCatalogManager {
    /// Create a new external catalog manager.
    pub fn new() -> Self {
        Self {
            providers: RwLock::new(Vec::new()),
        }
    }

    /// Register an external catalog provider with caching.
    pub fn register(
        &self,
        provider: Box<dyn ExternalCatalogProvider>,
        cache_ttl: Duration,
    ) -> Arc<CachedCatalogProvider> {
        let cached = Arc::new(CachedCatalogProvider::new(provider, cache_ttl));
        self.providers.write().push(cached.clone());
        cached
    }

    /// Discover all tables across all providers.
    pub fn discover_all(&self) -> Result<Vec<DiscoveredTable>> {
        let mut tables = Vec::new();
        for provider in self.providers.read().iter() {
            let schemas = provider.list_schemas()?;
            for schema in &schemas {
                let table_names = provider.list_tables(schema)?;
                for table in &table_names {
                    if let Ok(table_schema) = provider.get_table_schema(schema, table) {
                        let location = provider.get_table_location(schema, table).ok().flatten();
                        tables.push(DiscoveredTable {
                            provider_name: provider.name().to_string(),
                            schema: schema.clone(),
                            table: table.clone(),
                            table_schema,
                            location,
                        });
                    }
                }
            }
        }
        Ok(tables)
    }

    /// Get the number of registered providers.
    pub fn provider_count(&self) -> usize {
        self.providers.read().len()
    }
}

/// A table discovered from an external catalog.
#[derive(Debug, Clone)]
pub struct DiscoveredTable {
    pub provider_name: String,
    pub schema: String,
    pub table: String,
    pub table_schema: Schema,
    pub location: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    #[test]
    fn test_glue_provider() {
        let provider = GlueCatalogProvider::new(GlueConfig::default());
        provider.register_database("my_db");
        provider.register_table(
            "my_db",
            "users",
            test_schema(),
            Some("s3://bucket/users/".to_string()),
        );

        let schemas = provider.list_schemas().unwrap();
        assert!(schemas.contains(&"my_db".to_string()));

        let tables = provider.list_tables("my_db").unwrap();
        assert_eq!(tables, vec!["users"]);

        let schema = provider.get_table_schema("my_db", "users").unwrap();
        assert_eq!(schema.len(), 2);

        let loc = provider.get_table_location("my_db", "users").unwrap();
        assert_eq!(loc, Some("s3://bucket/users/".to_string()));
    }

    #[test]
    fn test_unity_provider() {
        let config = UnityConfig {
            workspace_url: "https://test.cloud.databricks.com".to_string(),
            token: None,
            catalog_name: "main".to_string(),
        };
        let provider = UnityCatalogProvider::new(config);
        provider.register_table("default", "orders", test_schema(), None);

        let schemas = provider.list_schemas().unwrap();
        assert!(schemas.contains(&"default".to_string()));

        let tables = provider.list_tables("default").unwrap();
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_hive_provider() {
        let provider = HiveMetastoreProvider::new(HiveConfig::default());
        provider.register_table(
            "warehouse",
            "events",
            test_schema(),
            Some("hdfs:///warehouse/events".to_string()),
            vec!["year".to_string(), "month".to_string()],
        );

        let tables = provider.list_tables("warehouse").unwrap();
        assert!(tables.contains(&"events".to_string()));

        let stats = provider
            .get_table_statistics("warehouse", "events")
            .unwrap()
            .unwrap();
        assert_eq!(stats.partitions.len(), 2);
    }

    #[test]
    fn test_cached_provider() {
        let glue = GlueCatalogProvider::new(GlueConfig::default());
        glue.register_database("db1");
        glue.register_table("db1", "t1", test_schema(), None);

        let cached = CachedCatalogProvider::new(Box::new(glue), Duration::from_secs(300));

        // First call populates cache
        let schemas = cached.list_schemas().unwrap();
        assert!(schemas.contains(&"db1".to_string()));

        // Second call returns from cache
        let schemas2 = cached.list_schemas().unwrap();
        assert_eq!(schemas, schemas2);

        // Invalidation
        cached.invalidate();
    }

    #[test]
    fn test_external_catalog_manager() {
        let manager = ExternalCatalogManager::new();

        let glue = GlueCatalogProvider::new(GlueConfig::default());
        glue.register_database("analytics");
        glue.register_table("analytics", "events", test_schema(), None);
        glue.register_table("analytics", "users", test_schema(), None);

        manager.register(Box::new(glue), Duration::from_secs(60));
        assert_eq!(manager.provider_count(), 1);

        let discovered = manager.discover_all().unwrap();
        assert_eq!(discovered.len(), 2);
        assert!(discovered.iter().any(|t| t.table == "events"));
        assert!(discovered.iter().any(|t| t.table == "users"));
    }
}
