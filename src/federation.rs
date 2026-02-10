//! External Table Federation
//!
//! Provides a framework for querying external data sources through a unified interface.
//! External tables are registered with the catalog and queried transparently via SQL.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use crate::catalog::TableProvider;
use crate::error::{BlazeError, Result};
use crate::types::{DataType, Schema};

/// External data source types
#[derive(Debug, Clone, PartialEq)]
pub enum ExternalSourceType {
    /// HTTP/REST API endpoint returning JSON
    Http { url: String, format: DataFormat },
    /// Local file with specific format
    File { path: String, format: DataFormat },
    /// In-memory data from another connection
    Memory { connection_id: String },
    /// Custom source with provider name
    Custom {
        provider: String,
        options: HashMap<String, String>,
    },
}

/// Data format for external sources
#[derive(Debug, Clone, PartialEq)]
pub enum DataFormat {
    Json,
    Csv,
    Parquet,
    ArrowIpc,
}

/// Configuration for an external table
#[derive(Debug, Clone)]
pub struct ExternalTableConfig {
    /// Name of the external table
    pub name: String,
    /// Source type
    pub source: ExternalSourceType,
    /// Optional schema override (if not provided, schema is inferred)
    pub schema: Option<Schema>,
    /// Cache TTL in seconds (0 = no caching)
    pub cache_ttl_secs: u64,
    /// Maximum rows to fetch
    pub max_rows: Option<usize>,
}

impl ExternalTableConfig {
    pub fn new(name: impl Into<String>, source: ExternalSourceType) -> Self {
        Self {
            name: name.into(),
            source,
            schema: None,
            cache_ttl_secs: 0,
            max_rows: None,
        }
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_cache_ttl(mut self, ttl_secs: u64) -> Self {
        self.cache_ttl_secs = ttl_secs;
        self
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(max_rows);
        self
    }
}

/// Trait for external data source providers
pub trait ExternalDataProvider: Send + Sync {
    /// Get the provider name
    fn name(&self) -> &str;

    /// Fetch data from the external source
    fn fetch(&self, config: &ExternalTableConfig) -> Result<Vec<RecordBatch>>;

    /// Infer schema from the external source
    fn infer_schema(&self, config: &ExternalTableConfig) -> Result<Schema>;

    /// Check if the provider supports predicate pushdown
    fn supports_pushdown(&self) -> bool {
        false
    }
}

/// File-based external data provider
pub struct FileProvider;

impl FileProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FileProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalDataProvider for FileProvider {
    fn name(&self) -> &str {
        "file"
    }

    fn fetch(&self, config: &ExternalTableConfig) -> Result<Vec<RecordBatch>> {
        match &config.source {
            ExternalSourceType::File { path, format } => match format {
                DataFormat::Csv => {
                    let table = crate::storage::CsvTable::open(path)?;
                    table.scan(None, &[], config.max_rows)
                }
                DataFormat::Parquet => {
                    let table = crate::storage::ParquetTable::open(path)?;
                    table.scan(None, &[], config.max_rows)
                }
                _ => Err(BlazeError::not_implemented(format!(
                    "File format {:?} not yet supported by FileProvider",
                    format
                ))),
            },
            _ => Err(BlazeError::invalid_argument(
                "FileProvider only handles File sources",
            )),
        }
    }

    fn infer_schema(&self, config: &ExternalTableConfig) -> Result<Schema> {
        match &config.source {
            ExternalSourceType::File { path, format } => match format {
                DataFormat::Csv => {
                    let table = crate::storage::CsvTable::open(path)?;
                    Ok(table.schema().clone())
                }
                DataFormat::Parquet => {
                    let table = crate::storage::ParquetTable::open(path)?;
                    Ok(table.schema().clone())
                }
                _ => Err(BlazeError::not_implemented(format!(
                    "Schema inference for file format {:?} not yet supported",
                    format
                ))),
            },
            _ => Err(BlazeError::invalid_argument(
                "FileProvider only handles File sources",
            )),
        }
    }
}

/// S3-compatible object storage provider.
/// Reads Parquet/CSV files from S3-compatible endpoints.
pub struct S3Provider {
    /// S3 endpoint URL (e.g., "https://s3.amazonaws.com")
    pub endpoint: String,
    /// AWS region
    pub region: String,
    /// Access key (optional - uses default chain if absent)
    pub access_key: Option<String>,
    /// Secret key
    pub secret_key: Option<String>,
}

impl S3Provider {
    pub fn new(endpoint: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            access_key: None,
            secret_key: None,
        }
    }

    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = Some(access_key.into());
        self.secret_key = Some(secret_key.into());
        self
    }

    /// Build the full S3 URL for a given path.
    fn build_url(&self, bucket: &str, key: &str) -> String {
        format!("{}/{}/{}", self.endpoint, bucket, key)
    }
}

impl ExternalDataProvider for S3Provider {
    fn name(&self) -> &str {
        "s3"
    }

    fn fetch(&self, config: &ExternalTableConfig) -> Result<Vec<RecordBatch>> {
        match &config.source {
            ExternalSourceType::Custom { options, .. } => {
                let bucket = options.get("bucket").ok_or_else(|| {
                    BlazeError::invalid_argument("S3 source requires 'bucket' option")
                })?;
                let key = options.get("key").ok_or_else(|| {
                    BlazeError::invalid_argument("S3 source requires 'key' option")
                })?;
                let format_str = options
                    .get("format")
                    .map(|s| s.as_str())
                    .unwrap_or("parquet");

                let url = self.build_url(bucket, key);

                // For local testing: if the "key" looks like a local path, read it directly
                if std::path::Path::new(key).exists() {
                    match format_str {
                        "parquet" | "pq" => {
                            let table = crate::storage::ParquetTable::open(key)?;
                            return table.scan(None, &[], config.max_rows);
                        }
                        "csv" => {
                            let table = crate::storage::CsvTable::open(key)?;
                            return table.scan(None, &[], config.max_rows);
                        }
                        _ => {}
                    }
                }

                Err(BlazeError::not_implemented(format!(
                    "S3 remote fetch not available (URL: {}). Use local file path for testing.",
                    url
                )))
            }
            _ => Err(BlazeError::invalid_argument(
                "S3Provider requires Custom source type",
            )),
        }
    }

    fn infer_schema(&self, config: &ExternalTableConfig) -> Result<Schema> {
        match &config.source {
            ExternalSourceType::Custom { options, .. } => {
                let key = options.get("key").ok_or_else(|| {
                    BlazeError::invalid_argument("S3 source requires 'key' option")
                })?;

                if std::path::Path::new(key).exists() {
                    let format_str = options
                        .get("format")
                        .map(|s| s.as_str())
                        .unwrap_or("parquet");
                    match format_str {
                        "parquet" | "pq" => {
                            let table = crate::storage::ParquetTable::open(key)?;
                            return Ok(table.schema().clone());
                        }
                        "csv" => {
                            let table = crate::storage::CsvTable::open(key)?;
                            return Ok(table.schema().clone());
                        }
                        _ => {}
                    }
                }

                Err(BlazeError::not_implemented("S3 remote schema inference"))
            }
            _ => Err(BlazeError::invalid_argument(
                "S3Provider requires Custom source type",
            )),
        }
    }
}

/// HTTP/REST API data provider.
/// Treats HTTP endpoints as tables by fetching JSON/CSV responses.
pub struct HttpProvider {
    /// Default timeout in seconds
    pub timeout_secs: u64,
    /// Default headers to include
    pub default_headers: HashMap<String, String>,
}

impl HttpProvider {
    pub fn new() -> Self {
        Self {
            timeout_secs: 30,
            default_headers: HashMap::new(),
        }
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.default_headers.insert(key.into(), value.into());
        self
    }
}

impl Default for HttpProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalDataProvider for HttpProvider {
    fn name(&self) -> &str {
        "http"
    }

    fn fetch(&self, config: &ExternalTableConfig) -> Result<Vec<RecordBatch>> {
        match &config.source {
            ExternalSourceType::Http { url, format } => {
                // For JSON format, we'd parse the response into RecordBatches
                // For now, return error with instructions
                Err(BlazeError::not_implemented(format!(
                    "HTTP fetch from {} (format: {:?}). Requires async HTTP client.",
                    url, format
                )))
            }
            ExternalSourceType::Custom { options, .. } => {
                let url = options.get("url").ok_or_else(|| {
                    BlazeError::invalid_argument("HTTP source requires 'url' option")
                })?;

                // If the URL is actually a local file, read it
                if std::path::Path::new(url).exists() {
                    let format_str = options.get("format").map(|s| s.as_str()).unwrap_or("json");
                    match format_str {
                        "csv" => {
                            let table = crate::storage::CsvTable::open(url)?;
                            return table.scan(None, &[], config.max_rows);
                        }
                        _ => {}
                    }
                }

                Err(BlazeError::not_implemented(format!(
                    "HTTP fetch from {}",
                    url
                )))
            }
            _ => Err(BlazeError::invalid_argument(
                "HttpProvider requires Http or Custom source type",
            )),
        }
    }

    fn infer_schema(&self, _config: &ExternalTableConfig) -> Result<Schema> {
        Err(BlazeError::not_implemented(
            "HTTP schema inference requires fetching a sample from the endpoint",
        ))
    }
}

/// PostgreSQL wire protocol connector.
/// Forwards queries to an external PostgreSQL-compatible database.
pub struct PostgresConnector {
    /// Connection string (e.g., "host=localhost port=5432 dbname=mydb")
    pub connection_string: String,
    /// Schema cache
    cached_schemas: std::sync::RwLock<HashMap<String, Schema>>,
}

impl PostgresConnector {
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            cached_schemas: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get the connection string.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Cache a schema for a table.
    pub fn cache_schema(&self, table: &str, schema: Schema) {
        if let Ok(mut cache) = self.cached_schemas.write() {
            cache.insert(table.to_string(), schema);
        }
    }
}

impl ExternalDataProvider for PostgresConnector {
    fn name(&self) -> &str {
        "postgres"
    }

    fn fetch(&self, config: &ExternalTableConfig) -> Result<Vec<RecordBatch>> {
        match &config.source {
            ExternalSourceType::Custom { options, .. } => {
                let _query = options
                    .get("query")
                    .or_else(|| options.get("table").map(|t| t))
                    .ok_or_else(|| {
                        BlazeError::invalid_argument(
                            "Postgres source requires 'query' or 'table' option",
                        )
                    })?;

                Err(BlazeError::not_implemented(
                    "PostgreSQL wire protocol connector requires tokio-postgres dependency",
                ))
            }
            _ => Err(BlazeError::invalid_argument(
                "PostgresConnector requires Custom source type",
            )),
        }
    }

    fn infer_schema(&self, config: &ExternalTableConfig) -> Result<Schema> {
        if let ExternalSourceType::Custom { options, .. } = &config.source {
            if let Some(table) = options.get("table") {
                if let Ok(cache) = self.cached_schemas.read() {
                    if let Some(schema) = cache.get(table) {
                        return Ok(schema.clone());
                    }
                }
            }
        }
        Err(BlazeError::not_implemented("PostgreSQL schema inference"))
    }

    fn supports_pushdown(&self) -> bool {
        true // PostgreSQL supports full SQL pushdown
    }
}
pub struct ExternalTable {
    config: ExternalTableConfig,
    schema: Schema,
    provider: Arc<dyn ExternalDataProvider>,
    /// Cached data
    cache: std::sync::RwLock<Option<CachedData>>,
}

struct CachedData {
    batches: Vec<RecordBatch>,
    cached_at: std::time::Instant,
}

impl ExternalTable {
    pub fn new(
        config: ExternalTableConfig,
        schema: Schema,
        provider: Arc<dyn ExternalDataProvider>,
    ) -> Self {
        Self {
            config,
            schema,
            provider,
            cache: std::sync::RwLock::new(None),
        }
    }

    fn get_data(&self) -> Result<Vec<RecordBatch>> {
        // Check cache
        if self.config.cache_ttl_secs > 0 {
            if let Ok(cache) = self.cache.read() {
                if let Some(cached) = cache.as_ref() {
                    if cached.cached_at.elapsed().as_secs() < self.config.cache_ttl_secs {
                        return Ok(cached.batches.clone());
                    }
                }
            }
        }

        // Fetch fresh data
        let batches = self.provider.fetch(&self.config)?;

        // Update cache
        if self.config.cache_ttl_secs > 0 {
            if let Ok(mut cache) = self.cache.write() {
                *cache = Some(CachedData {
                    batches: batches.clone(),
                    cached_at: std::time::Instant::now(),
                });
            }
        }

        Ok(batches)
    }
}

impl fmt::Debug for ExternalTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalTable")
            .field("config", &self.config)
            .field("schema", &self.schema)
            .finish()
    }
}

impl TableProvider for ExternalTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> crate::catalog::TableType {
        crate::catalog::TableType::External
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.get_data()?;

        // Apply projection if specified
        let batches = if let Some(indices) = projection {
            let projected: Result<Vec<RecordBatch>> = batches
                .iter()
                .map(|batch| {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(ArrowSchema::new(fields));
                    RecordBatch::try_new(schema, columns)
                        .map_err(|e| BlazeError::execution(e.to_string()))
                })
                .collect();
            projected?
        } else {
            batches
        };

        // Apply limit
        if let Some(max) = limit {
            let mut result = Vec::new();
            let mut rows = 0;
            for batch in batches {
                if rows >= max {
                    break;
                }
                let remaining = max - rows;
                if batch.num_rows() <= remaining {
                    rows += batch.num_rows();
                    result.push(batch);
                } else {
                    result.push(batch.slice(0, remaining));
                    rows += remaining;
                }
            }
            Ok(result)
        } else {
            Ok(batches)
        }
    }
}

/// Federation registry for managing external data providers
pub struct FederationRegistry {
    providers: HashMap<String, Arc<dyn ExternalDataProvider>>,
}

impl FederationRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            providers: HashMap::new(),
        };
        // Register built-in providers
        registry.register_provider(Arc::new(FileProvider::new()));
        registry
    }

    pub fn register_provider(&mut self, provider: Arc<dyn ExternalDataProvider>) {
        self.providers.insert(provider.name().to_string(), provider);
    }

    pub fn get_provider(&self, name: &str) -> Option<Arc<dyn ExternalDataProvider>> {
        self.providers.get(name).cloned()
    }

    pub fn create_external_table(&self, config: ExternalTableConfig) -> Result<ExternalTable> {
        let provider_name = match &config.source {
            ExternalSourceType::File { .. } => "file",
            ExternalSourceType::Http { .. } => "http",
            ExternalSourceType::Custom { provider, .. } => provider.as_str(),
            ExternalSourceType::Memory { .. } => "memory",
        };

        let provider = self.get_provider(provider_name).ok_or_else(|| {
            BlazeError::not_implemented(format!(
                "External data provider '{}' not registered",
                provider_name
            ))
        })?;

        let schema = if let Some(ref s) = config.schema {
            s.clone()
        } else {
            provider.infer_schema(&config)?
        };

        Ok(ExternalTable::new(config, schema, provider))
    }
}

impl Default for FederationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a predicate that can be shipped to a remote source.
#[derive(Debug, Clone)]
pub enum ShippablePredicate {
    /// Column equals a literal value
    Eq { column: String, value: String },
    /// Column not equal to a literal value
    NotEq { column: String, value: String },
    /// Column less than a literal value
    Lt { column: String, value: String },
    /// Column less than or equal
    LtEq { column: String, value: String },
    /// Column greater than a literal value
    Gt { column: String, value: String },
    /// Column greater than or equal
    GtEq { column: String, value: String },
    /// Column IS NULL
    IsNull { column: String },
    /// Column IS NOT NULL
    IsNotNull { column: String },
    /// AND combination
    And(Box<ShippablePredicate>, Box<ShippablePredicate>),
    /// OR combination
    Or(Box<ShippablePredicate>, Box<ShippablePredicate>),
}

impl ShippablePredicate {
    /// Convert to a SQL WHERE clause fragment.
    pub fn to_sql(&self) -> String {
        match self {
            Self::Eq { column, value } => format!("{} = {}", column, value),
            Self::NotEq { column, value } => format!("{} <> {}", column, value),
            Self::Lt { column, value } => format!("{} < {}", column, value),
            Self::LtEq { column, value } => format!("{} <= {}", column, value),
            Self::Gt { column, value } => format!("{} > {}", column, value),
            Self::GtEq { column, value } => format!("{} >= {}", column, value),
            Self::IsNull { column } => format!("{} IS NULL", column),
            Self::IsNotNull { column } => format!("{} IS NOT NULL", column),
            Self::And(left, right) => format!("({} AND {})", left.to_sql(), right.to_sql()),
            Self::Or(left, right) => format!("({} OR {})", left.to_sql(), right.to_sql()),
        }
    }
}

/// Cross-source query planner for federation.
/// Manages queries that span multiple data sources.
pub struct FederatedQueryPlanner {
    registry: FederationRegistry,
    /// Cached data from external sources with TTL
    cache: parking_lot::RwLock<HashMap<String, CachedResult>>,
}

struct CachedResult {
    data: Vec<RecordBatch>,
    cached_at: std::time::Instant,
    ttl: std::time::Duration,
}

impl CachedResult {
    fn is_valid(&self) -> bool {
        self.cached_at.elapsed() < self.ttl
    }
}

impl FederatedQueryPlanner {
    pub fn new(registry: FederationRegistry) -> Self {
        Self {
            registry,
            cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Fetch data from an external source with optional predicate shipping.
    pub fn fetch_with_predicates(
        &self,
        config: &ExternalTableConfig,
        predicates: &[ShippablePredicate],
    ) -> Result<Vec<RecordBatch>> {
        // Check cache first
        let cache_key = format!(
            "{}:{:?}",
            config.name,
            predicates.iter().map(|p| p.to_sql()).collect::<Vec<_>>()
        );
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.get(&cache_key) {
                if cached.is_valid() {
                    return Ok(cached.data.clone());
                }
            }
        }

        // Fetch from source
        let table = self.registry.create_external_table(config.clone())?;
        let data = table.scan(None, &[], config.max_rows)?;

        // Cache if TTL > 0
        if config.cache_ttl_secs > 0 {
            let mut cache = self.cache.write();
            cache.insert(
                cache_key,
                CachedResult {
                    data: data.clone(),
                    cached_at: std::time::Instant::now(),
                    ttl: std::time::Duration::from_secs(config.cache_ttl_secs),
                },
            );
        }

        Ok(data)
    }

    /// Invalidate cached data for a specific source.
    pub fn invalidate_cache(&self, source_name: &str) {
        let mut cache = self.cache.write();
        cache.retain(|key, _| !key.starts_with(source_name));
    }

    /// Clear all cached data.
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Get the registry.
    pub fn registry(&self) -> &FederationRegistry {
        &self.registry
    }

    /// Get a mutable reference to the registry.
    pub fn registry_mut(&mut self) -> &mut FederationRegistry {
        &mut self.registry
    }
}

/// A virtual table that federates queries across multiple external sources.
/// Enables cross-source joins by materializing remote data locally.
pub struct FederatedTable {
    /// Table name
    name: String,
    /// Schema of the federated table
    schema: Schema,
    /// External source configs that make up this table
    sources: Vec<ExternalTableConfig>,
    /// Federation planner
    planner: Arc<FederatedQueryPlanner>,
}

impl FederatedTable {
    pub fn new(
        name: impl Into<String>,
        schema: Schema,
        sources: Vec<ExternalTableConfig>,
        planner: Arc<FederatedQueryPlanner>,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            sources,
            planner,
        }
    }

    /// Materialize data from all sources into local batches.
    pub fn materialize(&self) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();
        for source in &self.sources {
            let batches = self.planner.fetch_with_predicates(source, &[])?;
            all_batches.extend(batches);
        }
        Ok(all_batches)
    }
}

impl fmt::Debug for FederatedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FederatedTable")
            .field("name", &self.name)
            .field("num_sources", &self.sources.len())
            .finish()
    }
}

impl TableProvider for FederatedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> crate::catalog::TableType {
        crate::catalog::TableType::External
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.materialize()?;

        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch in batches {
            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> = indices
                        .iter()
                        .filter_map(|&i| {
                            if i < batch.num_columns() {
                                Some(batch.column(i).clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .filter_map(|&i| batch.schema().fields().get(i).cloned())
                        .collect();
                    if columns.is_empty() {
                        continue;
                    }
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => batch,
            };

            if let Some(limit) = limit {
                let remaining = limit - rows_collected;
                if remaining == 0 {
                    break;
                }
                if projected.num_rows() <= remaining {
                    rows_collected += projected.num_rows();
                    result.push(projected);
                } else {
                    result.push(projected.slice(0, remaining));
                    break;
                }
            } else {
                result.push(projected);
            }
        }

        Ok(result)
    }
}

/// MySQL wire protocol connector.
/// Forwards queries to an external MySQL-compatible database.
pub struct MySQLConnector {
    /// Connection string (e.g., "mysql://user:pass@localhost:3306/mydb")
    pub connection_string: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Schema cache
    cached_schemas: std::sync::RwLock<HashMap<String, Schema>>,
}

impl MySQLConnector {
    /// Create a new MySQL connector with the given connection string.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            max_connections: 10,
            cached_schemas: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Set the maximum number of connections in the pool.
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Get the connection string.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Cache a schema for a table.
    pub fn cache_schema(&self, table: &str, schema: Schema) {
        if let Ok(mut cache) = self.cached_schemas.write() {
            cache.insert(table.to_string(), schema);
        }
    }
}

impl ExternalDataProvider for MySQLConnector {
    fn name(&self) -> &str {
        "mysql"
    }

    fn fetch(&self, config: &ExternalTableConfig) -> Result<Vec<RecordBatch>> {
        match &config.source {
            ExternalSourceType::Custom { options, .. } => {
                let _query = options
                    .get("query")
                    .or_else(|| options.get("table"))
                    .ok_or_else(|| {
                        BlazeError::invalid_argument(
                            "MySQL source requires 'query' or 'table' option",
                        )
                    })?;

                Err(BlazeError::not_implemented(
                    "MySQL wire protocol connector requires mysql_async dependency",
                ))
            }
            _ => Err(BlazeError::invalid_argument(
                "MySQLConnector requires Custom source type",
            )),
        }
    }

    fn infer_schema(&self, config: &ExternalTableConfig) -> Result<Schema> {
        if let ExternalSourceType::Custom { options, .. } = &config.source {
            if let Some(table) = options.get("table") {
                if let Ok(cache) = self.cached_schemas.read() {
                    if let Some(schema) = cache.get(table) {
                        return Ok(schema.clone());
                    }
                }
            }
        }
        Err(BlazeError::not_implemented("MySQL schema inference"))
    }

    fn supports_pushdown(&self) -> bool {
        true // MySQL supports SQL pushdown
    }
}

/// Result of pushdown analysis, classifying predicates into those that can be
/// shipped to a remote source and those that must be evaluated locally.
#[derive(Debug, Clone)]
pub struct PushdownResult {
    /// Predicates that can be pushed to the remote source
    pub shipped: Vec<ShippablePredicate>,
    /// Predicates that must be retained for local evaluation
    pub retained: Vec<ShippablePredicate>,
    /// Estimated selectivity of shipped predicates (0.0 to 1.0)
    pub selectivity_estimate: f64,
}

/// Analyzes logical predicates to determine what can be pushed to remote sources.
pub struct PushdownAnalyzer;

impl PushdownAnalyzer {
    /// Analyze a set of predicates and classify them as shippable or local-only.
    ///
    /// Predicates on simple column comparisons (Eq, NotEq, Lt, LtEq, Gt, GtEq,
    /// IsNull, IsNotNull) are shipped if the remote source `supports_pushdown`.
    /// Compound predicates (And, Or) are recursively analyzed.
    pub fn analyze_predicates(
        predicates: &[ShippablePredicate],
        supports_pushdown: bool,
    ) -> PushdownResult {
        if !supports_pushdown {
            return PushdownResult {
                shipped: Vec::new(),
                retained: predicates.to_vec(),
                selectivity_estimate: 1.0,
            };
        }

        let mut shipped = Vec::new();
        let mut retained = Vec::new();
        let mut selectivity = 1.0;

        for predicate in predicates {
            if Self::is_shippable(predicate) {
                selectivity *= Self::estimate_selectivity(predicate);
                shipped.push(predicate.clone());
            } else {
                retained.push(predicate.clone());
            }
        }

        PushdownResult {
            shipped,
            retained,
            selectivity_estimate: selectivity,
        }
    }

    /// Determine whether a predicate is safe to ship to a remote source.
    fn is_shippable(predicate: &ShippablePredicate) -> bool {
        match predicate {
            ShippablePredicate::Eq { .. }
            | ShippablePredicate::NotEq { .. }
            | ShippablePredicate::Lt { .. }
            | ShippablePredicate::LtEq { .. }
            | ShippablePredicate::Gt { .. }
            | ShippablePredicate::GtEq { .. }
            | ShippablePredicate::IsNull { .. }
            | ShippablePredicate::IsNotNull { .. } => true,
            ShippablePredicate::And(left, right) => {
                Self::is_shippable(left) && Self::is_shippable(right)
            }
            ShippablePredicate::Or(left, right) => {
                Self::is_shippable(left) && Self::is_shippable(right)
            }
        }
    }

    /// Estimate the selectivity of a predicate (fraction of rows passing).
    fn estimate_selectivity(predicate: &ShippablePredicate) -> f64 {
        match predicate {
            ShippablePredicate::Eq { .. } => 0.1,
            ShippablePredicate::NotEq { .. } => 0.9,
            ShippablePredicate::Lt { .. }
            | ShippablePredicate::LtEq { .. }
            | ShippablePredicate::Gt { .. }
            | ShippablePredicate::GtEq { .. } => 0.33,
            ShippablePredicate::IsNull { .. } => 0.05,
            ShippablePredicate::IsNotNull { .. } => 0.95,
            ShippablePredicate::And(left, right) => {
                Self::estimate_selectivity(left) * Self::estimate_selectivity(right)
            }
            ShippablePredicate::Or(left, right) => {
                let s1 = Self::estimate_selectivity(left);
                let s2 = Self::estimate_selectivity(right);
                (s1 + s2 - s1 * s2).min(1.0)
            }
        }
    }
}

/// Strategies for joining data across federated sources.
///
/// Each variant encodes a different network/compute tradeoff for cross-source joins.
#[derive(Debug, Clone, PartialEq)]
pub enum FederatedJoinStrategy {
    /// Broadcast the smaller table to the node holding the larger table.
    BroadcastSmallTable {
        /// Maximum row count for the smaller side to qualify for broadcast.
        threshold_rows: usize,
    },
    /// Ship a Bloom filter of the join keys to pre-filter the remote side.
    BloomFilterShip {
        /// Target false-positive probability.
        fpp: f64,
        /// Expected number of items in the filter.
        expected_items: usize,
    },
    /// Perform a semi-join reduction: fetch distinct keys first, then filter.
    SemiJoinReduction {
        /// Columns used as join keys.
        key_columns: Vec<String>,
    },
    /// Materialize both sides fully before joining locally.
    FullMaterialize,
}

impl FederatedJoinStrategy {
    /// Select the best join strategy given cardinality estimates.
    ///
    /// * If either side is below `broadcast_threshold`, broadcast it.
    /// * Otherwise, if the smaller side is at most 10% of the larger, use a
    ///   Bloom filter.
    /// * Otherwise, if explicit key columns are provided, use semi-join reduction.
    /// * Falls back to full materialization.
    pub fn select_strategy(
        left_rows: usize,
        right_rows: usize,
        key_columns: &[String],
        broadcast_threshold: usize,
    ) -> Self {
        let (smaller, _larger) = if left_rows <= right_rows {
            (left_rows, right_rows)
        } else {
            (right_rows, left_rows)
        };

        if smaller <= broadcast_threshold {
            return FederatedJoinStrategy::BroadcastSmallTable {
                threshold_rows: broadcast_threshold,
            };
        }

        let ratio = smaller as f64 / right_rows.max(left_rows).max(1) as f64;
        if ratio <= 0.1 {
            return FederatedJoinStrategy::BloomFilterShip {
                fpp: 0.01,
                expected_items: smaller,
            };
        }

        if !key_columns.is_empty() {
            return FederatedJoinStrategy::SemiJoinReduction {
                key_columns: key_columns.to_vec(),
            };
        }

        FederatedJoinStrategy::FullMaterialize
    }

    /// Estimate the number of bytes transferred over the network for this strategy.
    ///
    /// `avg_row_bytes` is the average serialized row size used for estimation.
    pub fn estimated_transfer_bytes(
        &self,
        left_rows: usize,
        right_rows: usize,
        avg_row_bytes: usize,
    ) -> usize {
        match self {
            FederatedJoinStrategy::BroadcastSmallTable { .. } => {
                left_rows.min(right_rows) * avg_row_bytes
            }
            FederatedJoinStrategy::BloomFilterShip {
                expected_items,
                fpp,
            } => {
                // Optimal Bloom filter size: -n * ln(p) / (ln2)^2  (in bits)
                let bits = -(*expected_items as f64) * fpp.ln() / (2.0_f64.ln().powi(2));
                (bits / 8.0).ceil() as usize
            }
            FederatedJoinStrategy::SemiJoinReduction { key_columns } => {
                // Transfer distinct keys (estimate 8 bytes per key column per row)
                let key_bytes = key_columns.len() * 8;
                left_rows.min(right_rows) * key_bytes
            }
            FederatedJoinStrategy::FullMaterialize => (left_rows + right_rows) * avg_row_bytes,
        }
    }
}

/// Parsed representation of a `CREATE FOREIGN TABLE` SQL statement.
///
/// Maps the DDL into an `ExternalTableConfig` that the federation registry
/// can use to create a new external table.
#[derive(Debug, Clone)]
pub struct CreateForeignTableStatement {
    /// Name of the foreign table to create.
    pub table_name: String,
    /// Column definitions: (name, data_type, nullable).
    pub columns: Vec<(String, DataType, bool)>,
    /// The foreign server / provider name (e.g., "postgres", "mysql").
    pub server: String,
    /// Provider-specific options (e.g., "table" -> "users", "schema" -> "public").
    pub options: HashMap<String, String>,
}

impl CreateForeignTableStatement {
    /// Create a new `CreateForeignTableStatement`.
    pub fn new(table_name: impl Into<String>, server: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            server: server.into(),
            options: HashMap::new(),
        }
    }

    /// Add a column definition.
    pub fn with_column(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        self.columns.push((name.into(), data_type, nullable));
        self
    }

    /// Add a provider option.
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Convert this statement into an `ExternalTableConfig`.
    pub fn to_external_config(&self) -> ExternalTableConfig {
        let schema = if self.columns.is_empty() {
            None
        } else {
            let fields: Vec<crate::types::Field> = self
                .columns
                .iter()
                .map(|(name, dt, nullable)| {
                    crate::types::Field::new(name.clone(), dt.clone(), *nullable)
                })
                .collect();
            Some(Schema::new(fields))
        };

        ExternalTableConfig {
            name: self.table_name.clone(),
            source: ExternalSourceType::Custom {
                provider: self.server.clone(),
                options: self.options.clone(),
            },
            schema,
            cache_ttl_secs: 0,
            max_rows: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Cost-based source selection and query routing
// ---------------------------------------------------------------------------

/// Capabilities advertised by a federated data source.
#[derive(Debug, Clone)]
pub struct SourceCapabilities {
    pub supports_filter_pushdown: bool,
    pub supports_projection_pushdown: bool,
    pub supports_aggregation_pushdown: bool,
    pub supports_sort_pushdown: bool,
    pub supports_limit_pushdown: bool,
    pub max_concurrent_queries: usize,
    pub estimated_latency_ms: u64,
    pub estimated_bandwidth_mbps: f64,
}

impl Default for SourceCapabilities {
    fn default() -> Self {
        Self {
            supports_filter_pushdown: true,
            supports_projection_pushdown: true,
            supports_aggregation_pushdown: false,
            supports_sort_pushdown: false,
            supports_limit_pushdown: true,
            max_concurrent_queries: 10,
            estimated_latency_ms: 50,
            estimated_bandwidth_mbps: 100.0,
        }
    }
}

/// Cost estimate for executing a query on a specific source.
#[derive(Debug, Clone)]
pub struct QueryCostEstimate {
    pub source_name: String,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub estimated_latency_ms: u64,
    pub network_cost: f64,
    pub compute_cost: f64,
}

impl QueryCostEstimate {
    pub fn total_cost(&self) -> f64 {
        self.network_cost + self.compute_cost
    }
}

/// Routes federated queries to the lowest-cost data source.
pub struct CostBasedRouter {
    source_capabilities: HashMap<String, SourceCapabilities>,
}

impl CostBasedRouter {
    pub fn new() -> Self {
        Self {
            source_capabilities: HashMap::new(),
        }
    }

    pub fn register_source(&mut self, name: impl Into<String>, capabilities: SourceCapabilities) {
        self.source_capabilities.insert(name.into(), capabilities);
    }

    /// Estimate the cost of running a query against the named source.
    pub fn estimate_cost(
        &self,
        source: &str,
        estimated_rows: u64,
        estimated_bytes: u64,
    ) -> Option<QueryCostEstimate> {
        let caps = self.source_capabilities.get(source)?;
        // Network cost: bytes transferred relative to bandwidth, plus base latency.
        let transfer_time_ms =
            (estimated_bytes as f64) / (caps.estimated_bandwidth_mbps * 125_000.0) * 1000.0;
        let network_cost = caps.estimated_latency_ms as f64 + transfer_time_ms;
        // Compute cost: proportional to rows processed.
        let compute_cost = estimated_rows as f64 * 0.001;
        Some(QueryCostEstimate {
            source_name: source.to_string(),
            estimated_rows,
            estimated_bytes,
            estimated_latency_ms: caps.estimated_latency_ms,
            network_cost,
            compute_cost,
        })
    }

    /// Select the source with the lowest total cost from a set of candidates.
    pub fn select_best_source(
        &self,
        candidates: &[String],
        estimated_rows: u64,
        estimated_bytes: u64,
    ) -> Option<String> {
        candidates
            .iter()
            .filter_map(|name| self.estimate_cost(name, estimated_rows, estimated_bytes))
            .min_by(|a, b| a.total_cost().partial_cmp(&b.total_cost()).unwrap())
            .map(|est| est.source_name)
    }

    pub fn available_sources(&self) -> Vec<String> {
        self.source_capabilities.keys().cloned().collect()
    }
}

/// Metrics for federated query execution monitoring.
#[derive(Debug, Clone, Default)]
pub struct FederationMetrics {
    pub total_federated_queries: u64,
    pub total_bytes_transferred: u64,
    pub total_rows_fetched: u64,
    pub cache_hit_count: u64,
    pub cache_miss_count: u64,
    pub avg_query_latency_ms: f64,
    pub source_errors: HashMap<String, u64>,
}

impl FederationMetrics {
    /// Returns the cache hit rate as a value between 0.0 and 1.0.
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hit_count + self.cache_miss_count;
        if total == 0 {
            0.0
        } else {
            self.cache_hit_count as f64 / total as f64
        }
    }

    /// Record a completed federated query, updating running averages.
    pub fn record_query(&mut self, bytes: u64, rows: u64, latency_ms: f64) {
        let prev_total = self.total_federated_queries as f64;
        self.total_federated_queries += 1;
        self.total_bytes_transferred += bytes;
        self.total_rows_fetched += rows;
        // Incremental average update.
        self.avg_query_latency_ms =
            (self.avg_query_latency_ms * prev_total + latency_ms) / self.total_federated_queries as f64;
    }

    /// Record an error from a specific source.
    pub fn record_error(&mut self, source: &str) {
        *self.source_errors.entry(source.to_string()).or_insert(0) += 1;
    }
}

// ---------------------------------------------------------------------------
// Federation Result Cache
// ---------------------------------------------------------------------------

/// Cache for federated query results with TTL-based expiration.
pub struct FederationResultCache {
    entries: HashMap<String, FederationCacheEntry>,
    ttl_secs: u64,
    max_entries: usize,
    hits: u64,
    misses: u64,
}

struct FederationCacheEntry {
    batches: Vec<RecordBatch>,
    inserted_at: std::time::Instant,
    access_count: u64,
}

impl FederationResultCache {
    pub fn new(ttl_secs: u64, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_secs,
            max_entries,
            hits: 0,
            misses: 0,
        }
    }

    /// Look up cached results for a query key.
    pub fn get(&mut self, key: &str) -> Option<&Vec<RecordBatch>> {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.inserted_at.elapsed().as_secs() < self.ttl_secs {
                entry.access_count += 1;
                self.hits += 1;
                return Some(&entry.batches);
            }
            // Expired
        }
        self.misses += 1;
        None
    }

    /// Store results in the cache.
    pub fn put(&mut self, key: String, batches: Vec<RecordBatch>) {
        if self.entries.len() >= self.max_entries {
            // Evict least recently used
            if let Some(lru_key) = self.entries.iter()
                .min_by_key(|(_, e)| e.access_count)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&lru_key);
            }
        }
        self.entries.insert(key, FederationCacheEntry {
            batches,
            inserted_at: std::time::Instant::now(),
            access_count: 0,
        });
    }

    /// Invalidate a specific cache entry.
    pub fn invalidate(&mut self, key: &str) {
        self.entries.remove(key);
    }

    /// Clear the entire cache.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Cache hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 { 0.0 } else { self.hits as f64 / total as f64 }
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Remove expired entries.
    pub fn evict_expired(&mut self) {
        let ttl = self.ttl_secs;
        self.entries.retain(|_, e| e.inserted_at.elapsed().as_secs() < ttl);
    }
}

/// Parallel query executor that fans out queries to multiple data sources.
pub struct ParallelQueryExecutor {
    max_concurrent: usize,
    timeout_ms: u64,
    results: Vec<SourceQueryResult>,
}

/// Result from querying a single source.
#[derive(Debug)]
pub struct SourceQueryResult {
    pub source_name: String,
    pub batches: Vec<RecordBatch>,
    pub execution_time_ms: u64,
    pub rows_returned: usize,
    pub error: Option<String>,
}

impl ParallelQueryExecutor {
    pub fn new(max_concurrent: usize, timeout_ms: u64) -> Self {
        Self {
            max_concurrent,
            timeout_ms,
            results: Vec::new(),
        }
    }

    /// Plan parallel execution for a federated query.
    pub fn plan_execution(&self, sources: &[&str]) -> Vec<ExecutionPlan> {
        sources.iter().enumerate().map(|(i, &source)| {
            ExecutionPlan {
                source: source.to_string(),
                priority: i,
                batch_idx: i / self.max_concurrent,
            }
        }).collect()
    }

    /// Add a result from a source.
    pub fn add_result(&mut self, result: SourceQueryResult) {
        self.results.push(result);
    }

    /// Get all results.
    pub fn results(&self) -> &[SourceQueryResult] {
        &self.results
    }

    /// Total rows across all source results.
    pub fn total_rows(&self) -> usize {
        self.results.iter().map(|r| r.rows_returned).sum()
    }

    /// Sources that returned errors.
    pub fn failed_sources(&self) -> Vec<&str> {
        self.results.iter()
            .filter(|r| r.error.is_some())
            .map(|r| r.source_name.as_str())
            .collect()
    }

    pub fn max_concurrent(&self) -> usize { self.max_concurrent }
    pub fn timeout_ms(&self) -> u64 { self.timeout_ms }
}

/// Execution plan for a single source query.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub source: String,
    pub priority: usize,
    pub batch_idx: usize,
}

// ---------------------------------------------------------------------------
// Phase 3: REST Connector + Credential Vault
// ---------------------------------------------------------------------------

/// REST API data connector that maps JSON responses to Arrow RecordBatches.
#[derive(Debug, Clone)]
pub struct RestApiConnector {
    pub base_url: String,
    pub headers: HashMap<String, String>,
    pub auth: RestAuth,
    pub response_path: Option<String>,
    pub timeout_ms: u64,
}

/// REST API authentication methods.
#[derive(Debug, Clone)]
pub enum RestAuth {
    None,
    Bearer(String),
    Basic { username: String, password: String },
    ApiKey { header: String, key: String },
}

impl RestApiConnector {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            headers: HashMap::new(),
            auth: RestAuth::None,
            response_path: None,
            timeout_ms: 30000,
        }
    }

    pub fn with_auth(mut self, auth: RestAuth) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_response_path(mut self, path: &str) -> Self {
        self.response_path = Some(path.to_string());
        self
    }

    /// Build the full URL for an endpoint.
    pub fn build_url(&self, endpoint: &str) -> String {
        if self.base_url.ends_with('/') || endpoint.starts_with('/') {
            format!("{}{}", self.base_url.trim_end_matches('/'), endpoint)
        } else {
            format!("{}/{}", self.base_url, endpoint)
        }
    }

    /// Get the authorization header value.
    pub fn auth_header(&self) -> Option<(String, String)> {
        match &self.auth {
            RestAuth::None => None,
            RestAuth::Bearer(token) => Some(("Authorization".to_string(), format!("Bearer {}", token))),
            RestAuth::Basic { username, password } => {
                let encoded = base64_encode(&format!("{}:{}", username, password));
                Some(("Authorization".to_string(), format!("Basic {}", encoded)))
            }
            RestAuth::ApiKey { header, key } => Some((header.clone(), key.clone())),
        }
    }
}

/// Simple base64 encoding (no external dependency).
fn base64_encode(input: &str) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Credential vault for securely storing data source credentials.
pub struct CredentialVault {
    credentials: HashMap<String, StoredCredential>,
}

/// A stored credential entry.
#[derive(Debug, Clone)]
pub struct StoredCredential {
    pub source_name: String,
    pub credential_type: CredentialType,
    pub created_at: std::time::SystemTime,
    pub expires_at: Option<std::time::SystemTime>,
}

/// Types of stored credentials.
#[derive(Debug, Clone)]
pub enum CredentialType {
    UsernamePassword { username: String, password: String },
    Token(String),
    AccessKey { key_id: String, secret: String },
    Certificate { cert_pem: String, key_pem: String },
}

impl CredentialVault {
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
        }
    }

    /// Store a credential for a data source.
    pub fn store(&mut self, name: &str, credential: CredentialType, expires_at: Option<std::time::SystemTime>) {
        self.credentials.insert(name.to_string(), StoredCredential {
            source_name: name.to_string(),
            credential_type: credential,
            created_at: std::time::SystemTime::now(),
            expires_at,
        });
    }

    /// Retrieve a credential by source name.
    pub fn get(&self, name: &str) -> Option<&StoredCredential> {
        self.credentials.get(name).and_then(|cred| {
            if let Some(expires) = cred.expires_at {
                if std::time::SystemTime::now() > expires {
                    return None; // Expired
                }
            }
            Some(cred)
        })
    }

    /// Remove a credential.
    pub fn remove(&mut self, name: &str) -> bool {
        self.credentials.remove(name).is_some()
    }

    /// List all stored credential names.
    pub fn list_sources(&self) -> Vec<&str> {
        self.credentials.keys().map(|k| k.as_str()).collect()
    }

    pub fn len(&self) -> usize { self.credentials.len() }
    pub fn is_empty(&self) -> bool { self.credentials.is_empty() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableProvider;
    use std::io::Write;

    #[test]
    fn test_external_table_config_builder() {
        let config = ExternalTableConfig::new(
            "test_table",
            ExternalSourceType::File {
                path: "/tmp/test.csv".to_string(),
                format: DataFormat::Csv,
            },
        )
        .with_cache_ttl(60)
        .with_max_rows(1000);

        assert_eq!(config.name, "test_table");
        assert_eq!(config.cache_ttl_secs, 60);
        assert_eq!(config.max_rows, Some(1000));
        assert!(config.schema.is_none());
    }

    #[test]
    fn test_federation_registry_default_providers() {
        let registry = FederationRegistry::new();
        assert!(registry.get_provider("file").is_some());
        assert!(registry.get_provider("http").is_none());
    }

    #[test]
    fn test_federation_registry_custom_provider() {
        let mut registry = FederationRegistry::new();

        struct MockProvider;
        impl ExternalDataProvider for MockProvider {
            fn name(&self) -> &str {
                "mock"
            }
            fn fetch(&self, _config: &ExternalTableConfig) -> Result<Vec<RecordBatch>> {
                Ok(vec![])
            }
            fn infer_schema(&self, _config: &ExternalTableConfig) -> Result<Schema> {
                Ok(Schema::empty())
            }
        }

        registry.register_provider(Arc::new(MockProvider));
        assert!(registry.get_provider("mock").is_some());
    }

    #[test]
    fn test_external_table_csv() {
        // Create a temporary CSV file
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "id,name,value").unwrap();
        writeln!(temp, "1,Alice,100").unwrap();
        writeln!(temp, "2,Bob,200").unwrap();
        writeln!(temp, "3,Charlie,300").unwrap();
        temp.flush().unwrap();

        let config = ExternalTableConfig::new(
            "test_csv",
            ExternalSourceType::File {
                path: temp.path().to_string_lossy().to_string(),
                format: DataFormat::Csv,
            },
        );

        let registry = FederationRegistry::new();
        let table = registry.create_external_table(config).unwrap();

        assert_eq!(table.schema().len(), 3);

        let batches = table.scan(None, &[], None).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_external_table_with_projection() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "id,name,value").unwrap();
        writeln!(temp, "1,Alice,100").unwrap();
        writeln!(temp, "2,Bob,200").unwrap();
        temp.flush().unwrap();

        let config = ExternalTableConfig::new(
            "test_proj",
            ExternalSourceType::File {
                path: temp.path().to_string_lossy().to_string(),
                format: DataFormat::Csv,
            },
        );

        let registry = FederationRegistry::new();
        let table = registry.create_external_table(config).unwrap();

        // Project only the first column
        let batches = table.scan(Some(&[0]), &[], None).unwrap();
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[test]
    fn test_external_table_with_limit() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "id,name").unwrap();
        writeln!(temp, "1,Alice").unwrap();
        writeln!(temp, "2,Bob").unwrap();
        writeln!(temp, "3,Charlie").unwrap();
        writeln!(temp, "4,Diana").unwrap();
        temp.flush().unwrap();

        let config = ExternalTableConfig::new(
            "test_limit",
            ExternalSourceType::File {
                path: temp.path().to_string_lossy().to_string(),
                format: DataFormat::Csv,
            },
        );

        let registry = FederationRegistry::new();
        let table = registry.create_external_table(config).unwrap();

        let batches = table.scan(None, &[], Some(2)).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_external_table_with_caching() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "id,value").unwrap();
        writeln!(temp, "1,100").unwrap();
        temp.flush().unwrap();

        let config = ExternalTableConfig::new(
            "test_cache",
            ExternalSourceType::File {
                path: temp.path().to_string_lossy().to_string(),
                format: DataFormat::Csv,
            },
        )
        .with_cache_ttl(300);

        let registry = FederationRegistry::new();
        let table = registry.create_external_table(config).unwrap();

        // First scan populates cache
        let batches1 = table.scan(None, &[], None).unwrap();
        assert_eq!(batches1.len(), 1);

        // Second scan should use cache
        let batches2 = table.scan(None, &[], None).unwrap();
        assert_eq!(batches2.len(), 1);
    }

    #[test]
    fn test_external_table_with_schema_override() {
        use crate::types::{DataType, Field};

        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "id,name").unwrap();
        writeln!(temp, "1,Alice").unwrap();
        temp.flush().unwrap();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let config = ExternalTableConfig::new(
            "test_schema",
            ExternalSourceType::File {
                path: temp.path().to_string_lossy().to_string(),
                format: DataFormat::Csv,
            },
        )
        .with_schema(schema.clone());

        let registry = FederationRegistry::new();
        let table = registry.create_external_table(config).unwrap();
        assert_eq!(table.schema().len(), 2);
        assert_eq!(table.schema().field(0).unwrap().name(), "id");
    }

    #[test]
    fn test_unregistered_provider_error() {
        let config = ExternalTableConfig::new(
            "test",
            ExternalSourceType::Http {
                url: "http://example.com/data".to_string(),
                format: DataFormat::Json,
            },
        );

        let registry = FederationRegistry::new();
        let result = registry.create_external_table(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_register_external_file_via_connection() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("test_data.csv");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&csv_path).unwrap();
            writeln!(f, "x,y").unwrap();
            writeln!(f, "10,20").unwrap();
            writeln!(f, "30,40").unwrap();
            f.flush().unwrap();
        }

        let conn = crate::Connection::in_memory().unwrap();
        conn.register_external_file("ext_table", &csv_path.to_string_lossy())
            .unwrap();

        let results = conn.query("SELECT * FROM ext_table").unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_shippable_predicate_to_sql() {
        let pred = ShippablePredicate::Eq {
            column: "id".to_string(),
            value: "42".to_string(),
        };
        assert_eq!(pred.to_sql(), "id = 42");

        let and_pred = ShippablePredicate::And(
            Box::new(ShippablePredicate::Gt {
                column: "age".to_string(),
                value: "18".to_string(),
            }),
            Box::new(ShippablePredicate::Lt {
                column: "age".to_string(),
                value: "65".to_string(),
            }),
        );
        assert_eq!(and_pred.to_sql(), "(age > 18 AND age < 65)");
    }

    #[test]
    fn test_federated_query_planner_cache() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "id,name").unwrap();
        writeln!(temp, "1,Alice").unwrap();
        temp.flush().unwrap();

        let registry = FederationRegistry::new();
        let planner = FederatedQueryPlanner::new(registry);

        let config = ExternalTableConfig::new(
            "cached_table",
            ExternalSourceType::File {
                path: temp.path().to_string_lossy().to_string(),
                format: DataFormat::Csv,
            },
        )
        .with_cache_ttl(300);

        // First fetch
        let data1 = planner.fetch_with_predicates(&config, &[]).unwrap();
        assert!(!data1.is_empty());

        // Second fetch should hit cache
        let data2 = planner.fetch_with_predicates(&config, &[]).unwrap();
        assert_eq!(data1.len(), data2.len());

        // Invalidate and refetch
        planner.invalidate_cache("cached_table");
    }

    #[test]
    fn test_federated_table() {
        use crate::types::{DataType, Field};

        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "id,name").unwrap();
        writeln!(temp, "1,Alice").unwrap();
        writeln!(temp, "2,Bob").unwrap();
        temp.flush().unwrap();

        let registry = FederationRegistry::new();
        let planner = Arc::new(FederatedQueryPlanner::new(registry));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);

        let source = ExternalTableConfig::new(
            "source1",
            ExternalSourceType::File {
                path: temp.path().to_string_lossy().to_string(),
                format: DataFormat::Csv,
            },
        );

        let fed_table = FederatedTable::new("fed", schema, vec![source], planner);
        let batches = fed_table.scan(None, &[], None).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_s3_provider_creation() {
        let provider = S3Provider::new("https://s3.amazonaws.com", "us-east-1")
            .with_credentials("AKID", "secret");
        assert_eq!(provider.name(), "s3");
        assert_eq!(provider.region, "us-east-1");
        assert!(provider.access_key.is_some());
        let url = provider.build_url("mybucket", "data/file.parquet");
        assert_eq!(url, "https://s3.amazonaws.com/mybucket/data/file.parquet");
    }

    #[test]
    fn test_s3_provider_local_file_fallback() {
        // Create a temp CSV file to simulate S3 local fallback
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(&path, "id,name\n1,Alice\n2,Bob\n").unwrap();

        let provider = S3Provider::new("http://localhost:9000", "local");
        let mut options = HashMap::new();
        options.insert("bucket".to_string(), "test".to_string());
        options.insert("key".to_string(), path.to_str().unwrap().to_string());
        options.insert("format".to_string(), "csv".to_string());

        let config = ExternalTableConfig::new(
            "s3test",
            ExternalSourceType::Custom {
                provider: "s3".to_string(),
                options,
            },
        );
        let result = provider.fetch(&config);
        assert!(result.is_ok());
        let batches = result.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_http_provider_creation() {
        let provider = HttpProvider::new()
            .with_timeout(60)
            .with_header("Authorization", "Bearer token");
        assert_eq!(provider.name(), "http");
        assert_eq!(provider.timeout_secs, 60);
        assert!(provider.default_headers.contains_key("Authorization"));
    }

    #[test]
    fn test_postgres_connector_creation() {
        let conn = PostgresConnector::new("host=localhost dbname=test");
        assert_eq!(conn.name(), "postgres");
        assert!(conn.supports_pushdown());
        assert_eq!(conn.connection_string(), "host=localhost dbname=test");
    }

    #[test]
    fn test_postgres_schema_cache() {
        let conn = PostgresConnector::new("host=localhost");
        let schema = Schema::new(vec![crate::types::Field::new(
            "id",
            crate::types::DataType::Int64,
            false,
        )]);
        conn.cache_schema("users", schema.clone());

        let mut options = HashMap::new();
        options.insert("table".to_string(), "users".to_string());
        let config = ExternalTableConfig::new(
            "pg_users",
            ExternalSourceType::Custom {
                provider: "postgres".to_string(),
                options,
            },
        );
        let inferred = conn.infer_schema(&config).unwrap();
        assert_eq!(inferred.fields().len(), 1);
    }

    #[test]
    fn test_mysql_connector_creation_and_cache() {
        use crate::types::Field;
        let conn =
            MySQLConnector::new("mysql://user:pass@localhost:3306/testdb").with_max_connections(20);
        assert_eq!(conn.name(), "mysql");
        assert!(conn.supports_pushdown());
        assert_eq!(
            conn.connection_string(),
            "mysql://user:pass@localhost:3306/testdb"
        );
        assert_eq!(conn.max_connections, 20);

        // Cache a schema and retrieve it
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        conn.cache_schema("users", schema.clone());

        let mut options = HashMap::new();
        options.insert("table".to_string(), "users".to_string());
        let config = ExternalTableConfig::new(
            "mysql_users",
            ExternalSourceType::Custom {
                provider: "mysql".to_string(),
                options,
            },
        );
        let inferred = conn.infer_schema(&config).unwrap();
        assert_eq!(inferred.fields().len(), 2);
    }

    #[test]
    fn test_pushdown_analyzer_with_pushdown_support() {
        let predicates = vec![
            ShippablePredicate::Eq {
                column: "id".to_string(),
                value: "42".to_string(),
            },
            ShippablePredicate::Gt {
                column: "age".to_string(),
                value: "18".to_string(),
            },
        ];

        let result = PushdownAnalyzer::analyze_predicates(&predicates, true);
        assert_eq!(result.shipped.len(), 2);
        assert!(result.retained.is_empty());
        // selectivity = 0.1 * 0.33 ≈ 0.033
        assert!(result.selectivity_estimate < 0.05);
        assert!(result.selectivity_estimate > 0.0);
    }

    #[test]
    fn test_pushdown_analyzer_without_pushdown_support() {
        let predicates = vec![ShippablePredicate::Eq {
            column: "id".to_string(),
            value: "1".to_string(),
        }];

        let result = PushdownAnalyzer::analyze_predicates(&predicates, false);
        assert!(result.shipped.is_empty());
        assert_eq!(result.retained.len(), 1);
        assert!((result.selectivity_estimate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_federated_join_strategy_selection() {
        // Small table → broadcast
        let strategy = FederatedJoinStrategy::select_strategy(50, 1_000_000, &[], 10_000);
        assert!(matches!(
            strategy,
            FederatedJoinStrategy::BroadcastSmallTable { .. }
        ));

        // 5% ratio → bloom filter
        let strategy = FederatedJoinStrategy::select_strategy(50_000, 1_000_000, &[], 1_000);
        assert!(matches!(
            strategy,
            FederatedJoinStrategy::BloomFilterShip { .. }
        ));

        // Larger sides with key columns → semi-join reduction
        let keys = vec!["user_id".to_string()];
        let strategy = FederatedJoinStrategy::select_strategy(500_000, 1_000_000, &keys, 1_000);
        assert!(matches!(
            strategy,
            FederatedJoinStrategy::SemiJoinReduction { .. }
        ));

        // No keys, similar sizes → full materialize
        let strategy = FederatedJoinStrategy::select_strategy(500_000, 1_000_000, &[], 1_000);
        assert!(matches!(strategy, FederatedJoinStrategy::FullMaterialize));

        // Estimated transfer bytes for broadcast
        let strat = FederatedJoinStrategy::BroadcastSmallTable {
            threshold_rows: 10_000,
        };
        let bytes = strat.estimated_transfer_bytes(100, 1_000_000, 256);
        assert_eq!(bytes, 100 * 256);
    }

    #[test]
    fn test_create_foreign_table_statement() {
        let stmt = CreateForeignTableStatement::new("remote_users", "postgres")
            .with_column("id", DataType::Int64, false)
            .with_column("name", DataType::Utf8, true)
            .with_column("active", DataType::Boolean, false)
            .with_option("table", "users")
            .with_option("schema", "public");

        assert_eq!(stmt.table_name, "remote_users");
        assert_eq!(stmt.server, "postgres");
        assert_eq!(stmt.columns.len(), 3);

        let config = stmt.to_external_config();
        assert_eq!(config.name, "remote_users");
        assert!(config.schema.is_some());

        let schema = config.schema.unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).unwrap().name(), "id");

        // Verify options are carried through
        if let ExternalSourceType::Custom { provider, options } = &config.source {
            assert_eq!(provider, "postgres");
            assert_eq!(options.get("table").unwrap(), "users");
            assert_eq!(options.get("schema").unwrap(), "public");
        } else {
            panic!("Expected Custom source type");
        }
    }

    #[test]
    fn test_source_capabilities_default() {
        let caps = SourceCapabilities::default();
        assert!(caps.supports_filter_pushdown);
        assert!(caps.supports_projection_pushdown);
        assert!(!caps.supports_aggregation_pushdown);
        assert!(!caps.supports_sort_pushdown);
        assert!(caps.supports_limit_pushdown);
        assert_eq!(caps.max_concurrent_queries, 10);
        assert_eq!(caps.estimated_latency_ms, 50);
        assert!((caps.estimated_bandwidth_mbps - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_query_cost_estimate_total() {
        let est = QueryCostEstimate {
            source_name: "s1".to_string(),
            estimated_rows: 1000,
            estimated_bytes: 4096,
            estimated_latency_ms: 10,
            network_cost: 5.0,
            compute_cost: 3.0,
        };
        assert!((est.total_cost() - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cost_based_router_selection() {
        let mut router = CostBasedRouter::new();
        // Fast source: low latency, high bandwidth
        router.register_source(
            "fast",
            SourceCapabilities {
                estimated_latency_ms: 5,
                estimated_bandwidth_mbps: 1000.0,
                ..Default::default()
            },
        );
        // Slow source: high latency, low bandwidth
        router.register_source(
            "slow",
            SourceCapabilities {
                estimated_latency_ms: 200,
                estimated_bandwidth_mbps: 10.0,
                ..Default::default()
            },
        );

        let candidates = vec!["fast".to_string(), "slow".to_string()];
        let best = router.select_best_source(&candidates, 1000, 1_000_000);
        assert_eq!(best, Some("fast".to_string()));

        // Verify available sources contains both
        let mut sources = router.available_sources();
        sources.sort();
        assert_eq!(sources, vec!["fast".to_string(), "slow".to_string()]);
    }

    #[test]
    fn test_cost_based_router_empty() {
        let router = CostBasedRouter::new();
        let candidates = vec!["missing".to_string()];
        assert!(router.select_best_source(&candidates, 100, 100).is_none());
        assert!(router.estimate_cost("missing", 100, 100).is_none());
        assert!(router.available_sources().is_empty());
    }

    #[test]
    fn test_federation_metrics_cache_hit_rate() {
        let mut m = FederationMetrics::default();
        // No hits or misses => 0.0
        assert!((m.cache_hit_rate() - 0.0).abs() < f64::EPSILON);

        m.cache_hit_count = 3;
        m.cache_miss_count = 1;
        assert!((m.cache_hit_rate() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_federation_metrics_record_query() {
        let mut m = FederationMetrics::default();
        m.record_query(1000, 50, 10.0);
        assert_eq!(m.total_federated_queries, 1);
        assert_eq!(m.total_bytes_transferred, 1000);
        assert_eq!(m.total_rows_fetched, 50);
        assert!((m.avg_query_latency_ms - 10.0).abs() < f64::EPSILON);

        m.record_query(2000, 100, 20.0);
        assert_eq!(m.total_federated_queries, 2);
        assert_eq!(m.total_bytes_transferred, 3000);
        assert_eq!(m.total_rows_fetched, 150);
        assert!((m.avg_query_latency_ms - 15.0).abs() < f64::EPSILON);

        m.record_error("pg");
        m.record_error("pg");
        m.record_error("mysql");
        assert_eq!(m.source_errors.get("pg"), Some(&2));
        assert_eq!(m.source_errors.get("mysql"), Some(&1));
    }

    // -----------------------------------------------------------------------
    // Federation Result Cache tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_federation_cache_hit_miss() {
        let mut cache = FederationResultCache::new(60, 100);
        assert!(cache.get("key1").is_none());
        assert_eq!(cache.hit_rate(), 0.0);

        cache.put("key1".to_string(), vec![]);
        assert!(cache.get("key1").is_some());
        assert!(cache.hit_rate() > 0.0);
    }

    #[test]
    fn test_federation_cache_invalidate() {
        let mut cache = FederationResultCache::new(60, 100);
        cache.put("key1".to_string(), vec![]);
        cache.invalidate("key1");
        assert!(cache.get("key1").is_none());
    }

    // -----------------------------------------------------------------------
    // Parallel Query Executor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parallel_executor_plan() {
        let exec = ParallelQueryExecutor::new(2, 5000);
        let plans = exec.plan_execution(&["postgres", "mysql", "sqlite"]);
        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].source, "postgres");
    }

    #[test]
    fn test_parallel_executor_results() {
        let mut exec = ParallelQueryExecutor::new(2, 5000);
        exec.add_result(SourceQueryResult {
            source_name: "pg".to_string(),
            batches: vec![],
            execution_time_ms: 100,
            rows_returned: 50,
            error: None,
        });
        exec.add_result(SourceQueryResult {
            source_name: "mysql".to_string(),
            batches: vec![],
            execution_time_ms: 200,
            rows_returned: 30,
            error: Some("timeout".to_string()),
        });
        assert_eq!(exec.total_rows(), 80);
        assert_eq!(exec.failed_sources(), vec!["mysql"]);
    }

    // -----------------------------------------------------------------------
    // REST Connector tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_rest_connector_url() {
        let conn = RestApiConnector::new("https://api.example.com");
        assert_eq!(conn.build_url("/users"), "https://api.example.com/users");
    }

    #[test]
    fn test_rest_connector_auth() {
        let conn = RestApiConnector::new("https://api.example.com")
            .with_auth(RestAuth::Bearer("token123".to_string()));
        let (header, value) = conn.auth_header().unwrap();
        assert_eq!(header, "Authorization");
        assert!(value.starts_with("Bearer "));
    }

    #[test]
    fn test_rest_connector_api_key() {
        let conn = RestApiConnector::new("https://api.example.com")
            .with_auth(RestAuth::ApiKey {
                header: "X-Api-Key".to_string(),
                key: "secret123".to_string(),
            });
        let (header, value) = conn.auth_header().unwrap();
        assert_eq!(header, "X-Api-Key");
        assert_eq!(value, "secret123");
    }

    // -----------------------------------------------------------------------
    // Credential Vault tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_credential_vault() {
        let mut vault = CredentialVault::new();
        vault.store("postgres", CredentialType::UsernamePassword {
            username: "admin".to_string(),
            password: "secret".to_string(),
        }, None);
        
        assert_eq!(vault.len(), 1);
        assert!(vault.get("postgres").is_some());
        assert!(vault.get("nonexistent").is_none());
    }

    #[test]
    fn test_credential_vault_remove() {
        let mut vault = CredentialVault::new();
        vault.store("pg", CredentialType::Token("tok".to_string()), None);
        assert!(vault.remove("pg"));
        assert!(vault.is_empty());
    }

    #[test]
    fn test_credential_vault_list() {
        let mut vault = CredentialVault::new();
        vault.store("pg", CredentialType::Token("t1".to_string()), None);
        vault.store("mysql", CredentialType::Token("t2".to_string()), None);
        let sources = vault.list_sources();
        assert_eq!(sources.len(), 2);
    }
}
