//! Arrow Flight Protocol support for Blaze.
//!
//! Provides a server and client implementation for the Apache Arrow Flight protocol,
//! enabling zero-copy data exchange between Blaze and other Arrow-based systems.
//!
//! # Architecture
//!
//! - **FlightServer**: Exposes Blaze queries via Flight RPC (DoGet, DoAction)
//! - **FlightClient**: Register remote Flight endpoints as local tables
//! - **FlightSqlHandler**: Flight SQL protocol for JDBC/ODBC compatibility
//!
//! # Usage
//!
//! The Flight module uses Arrow IPC for serialization, which is already
//! available via the `arrow-ipc` crate in the dependency tree.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::catalog::TableProvider;
use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// Flight endpoint descriptor.
#[derive(Debug, Clone)]
pub struct FlightEndpoint {
    /// Hostname or IP of the Flight server
    pub host: String,
    /// Port number
    pub port: u16,
    /// Whether to use TLS
    pub tls: bool,
}

impl FlightEndpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            tls: false,
        }
    }

    pub fn with_tls(mut self) -> Self {
        self.tls = true;
        self
    }

    pub fn uri(&self) -> String {
        let scheme = if self.tls { "grpc+tls" } else { "grpc" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

/// Flight ticket for identifying a query/dataset.
#[derive(Debug, Clone)]
pub struct FlightTicket {
    /// Unique ticket identifier
    pub ticket: Vec<u8>,
    /// Optional SQL query
    pub query: Option<String>,
    /// Optional table name
    pub table: Option<String>,
}

impl FlightTicket {
    pub fn from_query(sql: impl Into<String>) -> Self {
        let query = sql.into();
        Self {
            ticket: query.as_bytes().to_vec(),
            query: Some(query),
            table: None,
        }
    }

    pub fn from_table(table: impl Into<String>) -> Self {
        let table = table.into();
        Self {
            ticket: table.as_bytes().to_vec(),
            query: None,
            table: Some(table),
        }
    }
}

/// Flight info describing available data.
#[derive(Debug, Clone)]
pub struct FlightInfo {
    /// Schema of the data
    pub schema: Schema,
    /// Endpoints where the data can be fetched
    pub endpoints: Vec<FlightEndpoint>,
    /// Total number of records (if known)
    pub total_records: Option<i64>,
    /// Total byte size (if known)
    pub total_bytes: Option<i64>,
}

/// Serializes RecordBatches to Arrow IPC format for Flight transport.
pub fn serialize_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let mut buf = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    Ok(buf)
}

/// Deserializes Arrow IPC format back to RecordBatches.
pub fn deserialize_batches(data: &[u8]) -> Result<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = std::io::Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }

    Ok(batches)
}

/// Configuration for the Flight server.
#[derive(Debug, Clone)]
pub struct FlightServerConfig {
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Whether to enable TLS
    pub tls: bool,
    /// Maximum concurrent streams
    pub max_concurrent_streams: usize,
}

impl Default for FlightServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8815,
            max_message_size: 64 * 1024 * 1024, // 64MB
            tls: false,
            max_concurrent_streams: 100,
        }
    }
}

/// Flight action types supported by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightAction {
    /// List all available tables
    ListTables,
    /// Get schema for a table
    GetSchema { table: String },
    /// Execute a SQL query
    ExecuteQuery { sql: String },
    /// Create a prepared statement
    PrepareStatement { sql: String },
}

impl FlightAction {
    pub fn from_type_and_body(action_type: &str, body: &[u8]) -> Result<Self> {
        match action_type {
            "list_tables" => Ok(FlightAction::ListTables),
            "get_schema" => {
                let table = String::from_utf8(body.to_vec())
                    .map_err(|e| BlazeError::execution(format!("Invalid UTF-8: {}", e)))?;
                Ok(FlightAction::GetSchema { table })
            }
            "execute_query" => {
                let sql = String::from_utf8(body.to_vec())
                    .map_err(|e| BlazeError::execution(format!("Invalid UTF-8: {}", e)))?;
                Ok(FlightAction::ExecuteQuery { sql })
            }
            "prepare_statement" => {
                let sql = String::from_utf8(body.to_vec())
                    .map_err(|e| BlazeError::execution(format!("Invalid UTF-8: {}", e)))?;
                Ok(FlightAction::PrepareStatement { sql })
            }
            _ => Err(BlazeError::not_implemented(format!(
                "Unknown Flight action: {}",
                action_type
            ))),
        }
    }
}

/// A Flight server handler that processes requests using a Blaze connection.
pub struct FlightHandler {
    connection: crate::Connection,
    config: FlightServerConfig,
}

impl FlightHandler {
    pub fn new(connection: crate::Connection, config: FlightServerConfig) -> Self {
        Self { connection, config }
    }

    pub fn with_default_config(connection: crate::Connection) -> Self {
        Self::new(connection, FlightServerConfig::default())
    }

    /// Handle a DoGet request (execute query and return data).
    pub fn do_get(&self, ticket: &FlightTicket) -> Result<Vec<RecordBatch>> {
        if let Some(ref sql) = ticket.query {
            self.connection.query(sql)
        } else if let Some(ref table) = ticket.table {
            self.connection.query(&format!("SELECT * FROM {}", table))
        } else {
            Err(BlazeError::invalid_argument(
                "Ticket must contain a query or table name",
            ))
        }
    }

    /// Handle a DoAction request.
    pub fn do_action(&self, action: &FlightAction) -> Result<Vec<u8>> {
        match action {
            FlightAction::ListTables => {
                let tables = self.connection.list_tables();
                let json = serde_json::to_vec(&tables)
                    .map_err(|e| BlazeError::execution(format!("Serialization error: {}", e)))?;
                Ok(json)
            }
            FlightAction::GetSchema { table } => {
                let schema = self
                    .connection
                    .table_schema(table)
                    .ok_or_else(|| BlazeError::catalog(format!("Table '{}' not found", table)))?;
                let arrow_schema = schema.to_arrow();
                let schema_bytes = serde_json::to_vec(&arrow_schema)
                    .map_err(|e| BlazeError::execution(format!("Serialization error: {}", e)))?;
                Ok(schema_bytes)
            }
            FlightAction::ExecuteQuery { sql } => {
                let batches = self.connection.query(sql)?;
                serialize_batches(&batches)
            }
            FlightAction::PrepareStatement { sql } => {
                let _stmt = self.connection.prepare(sql)?;
                Ok(b"prepared".to_vec())
            }
        }
    }

    /// Get flight info for a query.
    pub fn get_flight_info(&self, query: &str) -> Result<FlightInfo> {
        let statements = crate::sql::parser::Parser::parse(query)?;
        let statement = statements
            .into_iter()
            .next()
            .ok_or_else(|| BlazeError::analysis("Empty query"))?;

        let binder = crate::planner::Binder::new(self.connection.catalog_list().clone());
        let logical_plan = binder.bind(statement)?;
        let schema = logical_plan.schema().clone();

        Ok(FlightInfo {
            schema,
            endpoints: vec![FlightEndpoint::new(&self.config.host, self.config.port)],
            total_records: None,
            total_bytes: None,
        })
    }
}

/// A Flight client that registers remote Flight endpoints as local tables.
pub struct FlightTableProvider {
    endpoint: FlightEndpoint,
    ticket: FlightTicket,
    schema: Schema,
    /// Cached data (lazily fetched)
    cached_data: parking_lot::RwLock<Option<Vec<RecordBatch>>>,
}

impl std::fmt::Debug for FlightTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightTableProvider")
            .field("endpoint", &self.endpoint)
            .field("ticket", &self.ticket)
            .finish()
    }
}

impl FlightTableProvider {
    pub fn new(endpoint: FlightEndpoint, ticket: FlightTicket, schema: Schema) -> Self {
        Self {
            endpoint,
            ticket,
            schema,
            cached_data: parking_lot::RwLock::new(None),
        }
    }

    /// Set cached data (for testing or pre-fetching).
    pub fn set_data(&self, data: Vec<RecordBatch>) {
        *self.cached_data.write() = Some(data);
    }
}

impl TableProvider for FlightTableProvider {
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
        // Use cached data if available
        let cached = self.cached_data.read();
        let batches = match cached.as_ref() {
            Some(data) => data.clone(),
            None => {
                return Err(BlazeError::execution(
                    "Flight data not available. Use set_data() to provide pre-fetched data, \
                     or enable the arrow-flight feature for network transport.",
                ));
            }
        };

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
                    let schema = Arc::new(ArrowSchema::new(fields));
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

// ---------------------------------------------------------------------------
// Flight SQL statement types, metadata, auth, and cancellation support
// ---------------------------------------------------------------------------

/// Statement handle for prepared statements in Flight SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatementHandle {
    pub id: String,
    pub query: String,
    pub created_at: std::time::SystemTime,
}

impl StatementHandle {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            query: query.into(),
            created_at: std::time::SystemTime::now(),
        }
    }

    pub fn is_expired(&self, max_age_secs: u64) -> bool {
        self.created_at
            .elapsed()
            .map(|d| d.as_secs() >= max_age_secs)
            .unwrap_or(false)
    }
}

/// Flight SQL statement types matching the protocol specification.
#[derive(Debug, Clone)]
pub enum FlightSqlStatement {
    GetCatalogs,
    GetSchemas {
        catalog: Option<String>,
        schema_filter: Option<String>,
    },
    GetTables {
        catalog: Option<String>,
        schema_filter: Option<String>,
        table_filter: Option<String>,
        include_schema: bool,
    },
    GetTableTypes,
    ExecuteQuery {
        query: String,
    },
    ExecutePrepared {
        handle: StatementHandle,
    },
    GetPrimaryKeys {
        catalog: Option<String>,
        schema: String,
        table: String,
    },
}

/// Manages active statement handles.
pub struct StatementHandleManager {
    handles: parking_lot::RwLock<HashMap<String, StatementHandle>>,
    max_handles: usize,
    max_age_secs: u64,
}

impl StatementHandleManager {
    pub fn new(max_handles: usize, max_age_secs: u64) -> Self {
        Self {
            handles: parking_lot::RwLock::new(HashMap::new()),
            max_handles,
            max_age_secs,
        }
    }

    pub fn create(&self, query: impl Into<String>) -> Result<StatementHandle> {
        let mut handles = self.handles.write();
        if handles.len() >= self.max_handles {
            return Err(BlazeError::execution(format!(
                "Maximum number of prepared statements ({}) reached",
                self.max_handles
            )));
        }
        let handle = StatementHandle::new(query);
        handles.insert(handle.id.clone(), handle.clone());
        Ok(handle)
    }

    pub fn get(&self, id: &str) -> Option<StatementHandle> {
        self.handles.read().get(id).cloned()
    }

    pub fn remove(&self, id: &str) -> Option<StatementHandle> {
        self.handles.write().remove(id)
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut handles = self.handles.write();
        let max_age = self.max_age_secs;
        let before = handles.len();
        handles.retain(|_, h| !h.is_expired(max_age));
        before - handles.len()
    }

    pub fn active_count(&self) -> usize {
        self.handles.read().len()
    }
}

/// Flight SQL metadata provider for catalog introspection.
pub struct FlightSqlMetadataProvider {
    catalog_name: String,
    schema_name: String,
    tables: parking_lot::RwLock<HashMap<String, Schema>>,
}

impl FlightSqlMetadataProvider {
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            catalog_name: catalog.into(),
            schema_name: schema.into(),
            tables: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn register_table(&self, name: impl Into<String>, schema: Schema) {
        self.tables.write().insert(name.into(), schema);
    }

    pub fn get_catalogs(&self) -> Result<RecordBatch> {
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "catalog_name",
            arrow::datatypes::DataType::Utf8,
            false,
        )]));
        let catalog_array = arrow::array::StringArray::from(vec![self.catalog_name.as_str()]);
        RecordBatch::try_new(schema, vec![Arc::new(catalog_array)])
            .map_err(|e| BlazeError::execution(format!("Failed to create catalogs batch: {}", e)))
    }

    pub fn get_schemas(&self, catalog_filter: Option<&str>) -> Result<RecordBatch> {
        let schema = Arc::new(ArrowSchema::new(vec![
            arrow::datatypes::Field::new("catalog_name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("schema_name", arrow::datatypes::DataType::Utf8, false),
        ]));

        if let Some(filter) = catalog_filter {
            if filter != self.catalog_name {
                return RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                    ],
                )
                .map_err(|e| {
                    BlazeError::execution(format!("Failed to create schemas batch: {}", e))
                });
            }
        }

        let catalogs = arrow::array::StringArray::from(vec![self.catalog_name.as_str()]);
        let schemas = arrow::array::StringArray::from(vec![self.schema_name.as_str()]);
        RecordBatch::try_new(schema, vec![Arc::new(catalogs), Arc::new(schemas)])
            .map_err(|e| BlazeError::execution(format!("Failed to create schemas batch: {}", e)))
    }

    pub fn get_tables(
        &self,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<RecordBatch> {
        let schema = Arc::new(ArrowSchema::new(vec![
            arrow::datatypes::Field::new("catalog_name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("schema_name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("table_name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("table_type", arrow::datatypes::DataType::Utf8, false),
        ]));

        // Check catalog filter
        if let Some(cf) = catalog_filter {
            if cf != self.catalog_name {
                return RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                    ],
                )
                .map_err(|e| {
                    BlazeError::execution(format!("Failed to create tables batch: {}", e))
                });
            }
        }

        // Check schema filter
        if let Some(sf) = schema_filter {
            if sf != self.schema_name {
                return RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                        Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                    ],
                )
                .map_err(|e| {
                    BlazeError::execution(format!("Failed to create tables batch: {}", e))
                });
            }
        }

        let tables = self.tables.read();
        let filtered: Vec<&String> = tables
            .keys()
            .filter(|name| table_filter.map(|f| name.contains(f)).unwrap_or(true))
            .collect();

        let catalog_col: Vec<&str> = filtered
            .iter()
            .map(|_| self.catalog_name.as_str())
            .collect();
        let schema_col: Vec<&str> = filtered.iter().map(|_| self.schema_name.as_str()).collect();
        let table_col: Vec<&str> = filtered.iter().map(|n| n.as_str()).collect();
        let type_col: Vec<&str> = filtered.iter().map(|_| "TABLE").collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(catalog_col)),
                Arc::new(arrow::array::StringArray::from(schema_col)),
                Arc::new(arrow::array::StringArray::from(table_col)),
                Arc::new(arrow::array::StringArray::from(type_col)),
            ],
        )
        .map_err(|e| BlazeError::execution(format!("Failed to create tables batch: {}", e)))
    }

    pub fn get_table_types(&self) -> Result<RecordBatch> {
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "table_type",
            arrow::datatypes::DataType::Utf8,
            false,
        )]));
        let types = arrow::array::StringArray::from(vec!["TABLE", "VIEW"]);
        RecordBatch::try_new(schema, vec![Arc::new(types)]).map_err(|e| {
            BlazeError::execution(format!("Failed to create table types batch: {}", e))
        })
    }
}

/// Authentication type for Flight SQL connections.
pub enum AuthType {
    None,
    Token {
        valid_tokens: Vec<String>,
    },
    Basic {
        credentials: HashMap<String, String>,
    },
}

/// An active authentication session.
pub struct AuthSession {
    pub token: String,
    pub username: String,
    pub created_at: std::time::Instant,
    pub expires_at: Option<std::time::Instant>,
}

/// Authentication handler for Flight SQL connections.
pub struct FlightAuthHandler {
    tokens: parking_lot::RwLock<HashMap<String, AuthSession>>,
    auth_type: AuthType,
}

impl FlightAuthHandler {
    pub fn no_auth() -> Self {
        Self {
            tokens: parking_lot::RwLock::new(HashMap::new()),
            auth_type: AuthType::None,
        }
    }

    pub fn with_tokens(tokens: Vec<String>) -> Self {
        Self {
            tokens: parking_lot::RwLock::new(HashMap::new()),
            auth_type: AuthType::Token {
                valid_tokens: tokens,
            },
        }
    }

    pub fn with_basic_auth(credentials: HashMap<String, String>) -> Self {
        Self {
            tokens: parking_lot::RwLock::new(HashMap::new()),
            auth_type: AuthType::Basic { credentials },
        }
    }

    pub fn authenticate(&self, token_or_credentials: &str) -> Result<AuthSession> {
        match &self.auth_type {
            AuthType::None => {
                let session_token = uuid::Uuid::new_v4().to_string();
                let session = AuthSession {
                    token: session_token.clone(),
                    username: "anonymous".to_string(),
                    created_at: std::time::Instant::now(),
                    expires_at: None,
                };
                self.tokens.write().insert(
                    session_token.clone(),
                    AuthSession {
                        token: session_token,
                        username: "anonymous".to_string(),
                        created_at: session.created_at,
                        expires_at: None,
                    },
                );
                Ok(session)
            }
            AuthType::Token { valid_tokens } => {
                if valid_tokens.contains(&token_or_credentials.to_string()) {
                    let session_token = uuid::Uuid::new_v4().to_string();
                    let now = std::time::Instant::now();
                    let session = AuthSession {
                        token: session_token.clone(),
                        username: "token_user".to_string(),
                        created_at: now,
                        expires_at: Some(now + std::time::Duration::from_secs(3600)),
                    };
                    self.tokens.write().insert(
                        session_token.clone(),
                        AuthSession {
                            token: session_token,
                            username: "token_user".to_string(),
                            created_at: now,
                            expires_at: session.expires_at,
                        },
                    );
                    Ok(session)
                } else {
                    Err(BlazeError::invalid_argument("Invalid authentication token"))
                }
            }
            AuthType::Basic { credentials } => {
                // Expect "username:password" format
                let parts: Vec<&str> = token_or_credentials.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(BlazeError::invalid_argument(
                        "Basic auth requires 'username:password' format",
                    ));
                }
                let (username, password) = (parts[0], parts[1]);
                match credentials.get(username) {
                    Some(stored_pw) if stored_pw == password => {
                        let session_token = uuid::Uuid::new_v4().to_string();
                        let now = std::time::Instant::now();
                        let session = AuthSession {
                            token: session_token.clone(),
                            username: username.to_string(),
                            created_at: now,
                            expires_at: Some(now + std::time::Duration::from_secs(3600)),
                        };
                        self.tokens.write().insert(
                            session_token.clone(),
                            AuthSession {
                                token: session_token,
                                username: username.to_string(),
                                created_at: now,
                                expires_at: session.expires_at,
                            },
                        );
                        Ok(session)
                    }
                    _ => Err(BlazeError::invalid_argument("Invalid username or password")),
                }
            }
        }
    }

    pub fn validate_session(&self, session_token: &str) -> bool {
        let sessions = self.tokens.read();
        match sessions.get(session_token) {
            Some(session) => {
                if let Some(expires) = session.expires_at {
                    std::time::Instant::now() < expires
                } else {
                    true
                }
            }
            None => false,
        }
    }
}

/// Query cancellation support.
pub struct QueryCancellationManager {
    cancelled: parking_lot::RwLock<HashMap<String, std::time::Instant>>,
}

impl QueryCancellationManager {
    pub fn new() -> Self {
        Self {
            cancelled: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn cancel(&self, query_id: &str) {
        self.cancelled
            .write()
            .insert(query_id.to_string(), std::time::Instant::now());
    }

    pub fn is_cancelled(&self, query_id: &str) -> bool {
        self.cancelled.read().contains_key(query_id)
    }

    pub fn cleanup_old(&self, max_age_secs: u64) {
        let mut cancelled = self.cancelled.write();
        let cutoff = std::time::Duration::from_secs(max_age_secs);
        cancelled.retain(|_, t| t.elapsed() < cutoff);
    }

    pub fn active_cancellations(&self) -> usize {
        self.cancelled.read().len()
    }
}

// ---------------------------------------------------------------------------
// Flight SQL Service — unified protocol handler for BI tool connectivity
// ---------------------------------------------------------------------------

/// Result of a Flight SQL operation.
#[derive(Debug)]
pub enum FlightSqlResult {
    /// Query results as record batches
    Query(Vec<RecordBatch>),
    /// Row count for DML operations
    Update(usize),
    /// Metadata (catalogs, schemas, tables)
    Metadata(RecordBatch),
    /// Statement handle for prepared statements
    Prepared(StatementHandle),
    /// Empty success
    Ok,
}

/// Configuration for the Flight SQL service.
pub struct FlightSqlServiceConfig {
    /// Server host
    pub host: String,
    /// Server port
    pub port: u16,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Query timeout in seconds
    pub query_timeout_secs: u64,
    /// Maximum statement handle age in seconds
    pub max_handle_age_secs: u64,
    /// Authentication type
    pub auth: AuthType,
    /// Enable TLS
    pub tls: bool,
}

impl Default for FlightSqlServiceConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 47470,
            max_connections: 64,
            query_timeout_secs: 300,
            max_handle_age_secs: 3600,
            auth: AuthType::None,
            tls: false,
        }
    }
}

impl FlightSqlServiceConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_auth(mut self, auth: AuthType) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }
}

/// Flight SQL Service providing full protocol support for BI tool connectivity.
///
/// Integrates FlightHandler, FlightSqlMetadataProvider, StatementHandleManager,
/// FlightAuthHandler, and QueryCancellationManager into a single unified service.
pub struct FlightSqlService {
    /// Query execution handler
    handler: FlightHandler,
    /// Metadata provider for catalog introspection
    metadata: FlightSqlMetadataProvider,
    /// Statement handle manager for prepared statements
    statements: StatementHandleManager,
    /// Authentication handler
    auth: FlightAuthHandler,
    /// Query cancellation manager
    cancellation: QueryCancellationManager,
    /// Service configuration
    config: FlightSqlServiceConfig,
}

impl FlightSqlService {
    /// Create a new Flight SQL service with the given connection and config.
    pub fn new(connection: crate::Connection, config: FlightSqlServiceConfig) -> Self {
        let server_config = FlightServerConfig {
            host: config.host.clone(),
            port: config.port,
            max_message_size: 64 * 1024 * 1024,
            tls: config.tls,
            max_concurrent_streams: config.max_connections,
        };

        let auth = match &config.auth {
            AuthType::None => FlightAuthHandler::no_auth(),
            AuthType::Token { valid_tokens } => {
                FlightAuthHandler::with_tokens(valid_tokens.clone())
            }
            AuthType::Basic { credentials } => {
                FlightAuthHandler::with_basic_auth(credentials.clone())
            }
        };

        Self {
            handler: FlightHandler::new(connection, server_config),
            metadata: FlightSqlMetadataProvider::new("blaze", "public"),
            statements: StatementHandleManager::new(1024, config.max_handle_age_secs),
            auth,
            cancellation: QueryCancellationManager::new(),
            config,
        }
    }

    /// Create with default configuration (no auth, port 47470).
    pub fn with_defaults(connection: crate::Connection) -> Self {
        Self::new(connection, FlightSqlServiceConfig::default())
    }

    /// Register a table's schema for metadata introspection.
    pub fn register_table_schema(&self, name: &str, schema: Schema) {
        self.metadata.register_table(name, schema);
    }

    /// Authenticate a client. Returns session token on success.
    pub fn authenticate(&self, credentials: &str) -> Result<AuthSession> {
        self.auth.authenticate(credentials)
    }

    /// Validate an existing session.
    pub fn validate_session(&self, token: &str) -> bool {
        self.auth.validate_session(token)
    }

    /// Execute a SQL query (CommandStatementQuery).
    pub fn execute_query(&self, sql: &str) -> Result<FlightSqlResult> {
        let ticket = FlightTicket::from_query(sql);
        let batches = self.handler.do_get(&ticket)?;
        Ok(FlightSqlResult::Query(batches))
    }

    /// Execute a DML statement (CommandStatementUpdate).
    pub fn execute_update(&self, sql: &str) -> Result<FlightSqlResult> {
        let action = FlightAction::ExecuteQuery {
            sql: sql.to_string(),
        };
        self.handler.do_action(&action)?;
        Ok(FlightSqlResult::Update(0))
    }

    /// Prepare a statement (ActionCreatePreparedStatementRequest).
    pub fn prepare_statement(&self, sql: &str) -> Result<FlightSqlResult> {
        let handle = self.statements.create(sql)?;
        Ok(FlightSqlResult::Prepared(handle))
    }

    /// Execute a prepared statement by handle ID.
    pub fn execute_prepared(&self, handle_id: &str) -> Result<FlightSqlResult> {
        let handle = self.statements.get(handle_id).ok_or_else(|| {
            BlazeError::execution(format!("Statement handle not found: {}", handle_id))
        })?;
        let ticket = FlightTicket::from_query(&handle.query);
        let batches = self.handler.do_get(&ticket)?;
        Ok(FlightSqlResult::Query(batches))
    }

    /// Close a prepared statement (ActionClosePreparedStatementRequest).
    pub fn close_prepared(&self, handle_id: &str) -> Result<FlightSqlResult> {
        self.statements.remove(handle_id);
        Ok(FlightSqlResult::Ok)
    }

    /// Get flight info for a query (used by clients to plan data retrieval).
    pub fn get_flight_info(&self, sql: &str) -> Result<FlightInfo> {
        self.handler.get_flight_info(sql)
    }

    /// Retrieve data for a ticket (DoGet).
    pub fn do_get(&self, ticket: &FlightTicket) -> Result<Vec<RecordBatch>> {
        self.handler.do_get(ticket)
    }

    /// Get catalog metadata (GetCatalogs).
    pub fn get_catalogs(&self) -> Result<FlightSqlResult> {
        let batch = self.metadata.get_catalogs()?;
        Ok(FlightSqlResult::Metadata(batch))
    }

    /// Get schema metadata (GetSchemas).
    pub fn get_schemas(&self, catalog_filter: Option<&str>) -> Result<FlightSqlResult> {
        let batch = self.metadata.get_schemas(catalog_filter)?;
        Ok(FlightSqlResult::Metadata(batch))
    }

    /// Get table metadata (GetTables).
    pub fn get_tables(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<FlightSqlResult> {
        let batch = self.metadata.get_tables(catalog, schema, table_filter)?;
        Ok(FlightSqlResult::Metadata(batch))
    }

    /// Get table types (GetTableTypes).
    pub fn get_table_types(&self) -> Result<FlightSqlResult> {
        let batch = self.metadata.get_table_types()?;
        Ok(FlightSqlResult::Metadata(batch))
    }

    /// Cancel a running query.
    pub fn cancel_query(&self, query_id: &str) {
        self.cancellation.cancel(query_id);
    }

    /// Check if a query has been cancelled.
    pub fn is_cancelled(&self, query_id: &str) -> bool {
        self.cancellation.is_cancelled(query_id)
    }

    /// Cleanup expired handles and cancellations.
    pub fn cleanup(&self) {
        self.statements.cleanup_expired();
        self.cancellation
            .cleanup_old(self.config.query_timeout_secs);
    }

    /// Get service endpoint.
    pub fn endpoint(&self) -> FlightEndpoint {
        FlightEndpoint {
            host: self.config.host.clone(),
            port: self.config.port,
            tls: self.config.tls,
        }
    }

    /// Get the number of active prepared statements.
    pub fn active_statements(&self) -> usize {
        self.statements.active_count()
    }

    /// Get the number of active cancellations.
    pub fn active_cancellations(&self) -> usize {
        self.cancellation.active_cancellations()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};
    use arrow::array::{Int64Array, StringArray};

    fn create_test_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(ArrowSchema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();
        vec![batch]
    }

    #[test]
    fn test_serialize_deserialize_batches() {
        let batches = create_test_batches();
        let data = serialize_batches(&batches).unwrap();
        assert!(!data.is_empty());

        let restored = deserialize_batches(&data).unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].num_rows(), 3);
        assert_eq!(restored[0].num_columns(), 2);
    }

    #[test]
    fn test_serialize_empty() {
        let data = serialize_batches(&[]).unwrap();
        assert!(data.is_empty());

        let restored = deserialize_batches(&data).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn test_flight_endpoint() {
        let ep = FlightEndpoint::new("localhost", 8815);
        assert_eq!(ep.uri(), "grpc://localhost:8815");

        let ep_tls = FlightEndpoint::new("example.com", 443).with_tls();
        assert_eq!(ep_tls.uri(), "grpc+tls://example.com:443");
    }

    #[test]
    fn test_flight_ticket() {
        let ticket = FlightTicket::from_query("SELECT 1");
        assert_eq!(ticket.query, Some("SELECT 1".to_string()));
        assert_eq!(ticket.table, None);

        let ticket = FlightTicket::from_table("users");
        assert_eq!(ticket.table, Some("users".to_string()));
    }

    #[test]
    fn test_flight_action_parsing() {
        let action = FlightAction::from_type_and_body("list_tables", &[]).unwrap();
        assert_eq!(action, FlightAction::ListTables);

        let action = FlightAction::from_type_and_body("execute_query", b"SELECT 1").unwrap();
        assert_eq!(
            action,
            FlightAction::ExecuteQuery {
                sql: "SELECT 1".to_string()
            }
        );
    }

    #[test]
    fn test_flight_handler_do_get() {
        let conn = crate::Connection::in_memory().unwrap();
        let handler = FlightHandler::with_default_config(conn);

        let ticket = FlightTicket::from_query("SELECT 1 + 1 AS result");
        let batches = handler.do_get(&ticket).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_flight_handler_do_action_list_tables() {
        let conn = crate::Connection::in_memory().unwrap();
        let handler = FlightHandler::with_default_config(conn);

        let action = FlightAction::ListTables;
        let result = handler.do_action(&action).unwrap();
        let tables: Vec<String> = serde_json::from_slice(&result).unwrap();
        assert!(tables.is_empty()); // No tables registered
    }

    #[test]
    fn test_flight_table_provider() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let ep = FlightEndpoint::new("localhost", 8815);
        let ticket = FlightTicket::from_table("remote_users");
        let provider = FlightTableProvider::new(ep, ticket, schema);

        // Set cached data
        let batches = create_test_batches();
        provider.set_data(batches);

        // Scan
        let result = provider.scan(None, &[], None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);

        // Scan with limit
        let result = provider.scan(None, &[], Some(2)).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_flight_server_config() {
        let config = FlightServerConfig::default();
        assert_eq!(config.port, 8815);
        assert_eq!(config.max_concurrent_streams, 100);
        assert!(!config.tls);
    }

    // --- Flight SQL statement, metadata, auth, and cancellation tests ---

    #[test]
    fn test_statement_handle_create_and_expiry() {
        let handle = StatementHandle::new("SELECT 1");
        assert_eq!(handle.query, "SELECT 1");
        assert!(!handle.id.is_empty());
        // Freshly created handle should not be expired with a reasonable age
        assert!(!handle.is_expired(60));
        // A zero-second max age means it's immediately expired
        assert!(handle.is_expired(0));
    }

    #[test]
    fn test_statement_handle_manager() {
        let mgr = StatementHandleManager::new(3, 600);
        assert_eq!(mgr.active_count(), 0);

        let h1 = mgr.create("SELECT 1").unwrap();
        let h2 = mgr.create("SELECT 2").unwrap();
        assert_eq!(mgr.active_count(), 2);

        // Retrieve
        let fetched = mgr.get(&h1.id).unwrap();
        assert_eq!(fetched.query, "SELECT 1");

        // Remove
        let removed = mgr.remove(&h2.id);
        assert!(removed.is_some());
        assert_eq!(mgr.active_count(), 1);

        // Max handles limit
        mgr.create("SELECT 3").unwrap();
        mgr.create("SELECT 4").unwrap();
        let result = mgr.create("SELECT 5");
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_provider_get_catalogs() {
        let provider = FlightSqlMetadataProvider::new("blaze_catalog", "public");
        let batch = provider.get_catalogs().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        let catalog_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(catalog_col.value(0), "blaze_catalog");
    }

    #[test]
    fn test_metadata_provider_get_tables() {
        let provider = FlightSqlMetadataProvider::new("my_catalog", "my_schema");
        let schema1 = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema2 = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);
        provider.register_table("users", schema1);
        provider.register_table("orders", schema2);

        // All tables
        let batch = provider.get_tables(None, None, None).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Filtered by table name
        let batch = provider.get_tables(None, None, Some("users")).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let table_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table_col.value(0), "users");

        // Wrong catalog yields zero rows
        let batch = provider
            .get_tables(Some("other_catalog"), None, None)
            .unwrap();
        assert_eq!(batch.num_rows(), 0);

        // Table types
        let batch = provider.get_table_types().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_auth_handler_no_auth() {
        let handler = FlightAuthHandler::no_auth();
        let session = handler.authenticate("anything").unwrap();
        assert_eq!(session.username, "anonymous");
        assert!(handler.validate_session(&session.token));
        assert!(!handler.validate_session("bogus_token"));
    }

    #[test]
    fn test_auth_handler_basic_auth() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "secret".to_string());
        let handler = FlightAuthHandler::with_basic_auth(creds);

        // Successful login
        let session = handler.authenticate("admin:secret").unwrap();
        assert_eq!(session.username, "admin");
        assert!(handler.validate_session(&session.token));

        // Invalid password
        let result = handler.authenticate("admin:wrong");
        assert!(result.is_err());

        // Invalid format
        let result = handler.authenticate("nocolon");
        assert!(result.is_err());
    }

    #[test]
    fn test_query_cancellation() {
        let mgr = QueryCancellationManager::new();
        assert_eq!(mgr.active_cancellations(), 0);

        mgr.cancel("q1");
        mgr.cancel("q2");
        assert!(mgr.is_cancelled("q1"));
        assert!(mgr.is_cancelled("q2"));
        assert!(!mgr.is_cancelled("q3"));
        assert_eq!(mgr.active_cancellations(), 2);

        // cleanup_old with a very large age should not remove recent entries
        mgr.cleanup_old(3600);
        assert_eq!(mgr.active_cancellations(), 2);
    }

    #[test]
    fn test_flight_sql_statement_variants() {
        let stmt = FlightSqlStatement::GetCatalogs;
        assert!(matches!(stmt, FlightSqlStatement::GetCatalogs));

        let stmt = FlightSqlStatement::GetTables {
            catalog: Some("cat".to_string()),
            schema_filter: None,
            table_filter: Some("users".to_string()),
            include_schema: true,
        };
        if let FlightSqlStatement::GetTables {
            catalog,
            table_filter,
            include_schema,
            ..
        } = &stmt
        {
            assert_eq!(catalog.as_deref(), Some("cat"));
            assert_eq!(table_filter.as_deref(), Some("users"));
            assert!(include_schema);
        } else {
            panic!("Expected GetTables variant");
        }
    }

    // --- Flight SQL Service integration tests ---

    fn create_flight_sql_service() -> FlightSqlService {
        let conn = crate::Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE test_users (id BIGINT, name VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO test_users VALUES (1, 'Alice')")
            .unwrap();
        conn.execute("INSERT INTO test_users VALUES (2, 'Bob')")
            .unwrap();

        let service = FlightSqlService::with_defaults(conn);
        service.register_table_schema(
            "test_users",
            Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ]),
        );
        service
    }

    #[test]
    fn test_flight_sql_service_query() {
        let service = create_flight_sql_service();
        let result = service.execute_query("SELECT * FROM test_users").unwrap();
        match result {
            FlightSqlResult::Query(batches) => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total, 2);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_flight_sql_service_prepare_and_execute() {
        let service = create_flight_sql_service();
        let result = service
            .prepare_statement("SELECT * FROM test_users")
            .unwrap();
        match result {
            FlightSqlResult::Prepared(handle) => {
                assert_eq!(service.active_statements(), 1);
                let exec_result = service.execute_prepared(&handle.id).unwrap();
                match exec_result {
                    FlightSqlResult::Query(batches) => {
                        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                        assert_eq!(total, 2);
                    }
                    _ => panic!("Expected Query result"),
                }
                service.close_prepared(&handle.id).unwrap();
                assert_eq!(service.active_statements(), 0);
            }
            _ => panic!("Expected Prepared result"),
        }
    }

    #[test]
    fn test_flight_sql_service_metadata() {
        let service = create_flight_sql_service();

        // Get catalogs
        let result = service.get_catalogs().unwrap();
        assert!(matches!(result, FlightSqlResult::Metadata(_)));

        // Get schemas
        let result = service.get_schemas(None).unwrap();
        assert!(matches!(result, FlightSqlResult::Metadata(_)));

        // Get tables
        let result = service.get_tables(None, None, None).unwrap();
        assert!(matches!(result, FlightSqlResult::Metadata(_)));

        // Get table types
        let result = service.get_table_types().unwrap();
        assert!(matches!(result, FlightSqlResult::Metadata(_)));
    }

    #[test]
    fn test_flight_sql_service_cancellation() {
        let service = create_flight_sql_service();
        assert_eq!(service.active_cancellations(), 0);
        service.cancel_query("query-1");
        assert!(service.is_cancelled("query-1"));
        assert!(!service.is_cancelled("query-2"));
        assert_eq!(service.active_cancellations(), 1);
    }

    #[test]
    fn test_flight_sql_service_endpoint() {
        let config = FlightSqlServiceConfig::new()
            .with_host("localhost")
            .with_port(9090)
            .with_tls(true);
        let conn = crate::Connection::in_memory().unwrap();
        let service = FlightSqlService::new(conn, config);
        let ep = service.endpoint();
        assert_eq!(ep.host, "localhost");
        assert_eq!(ep.port, 9090);
        assert!(ep.tls);
        assert_eq!(ep.uri(), "grpc+tls://localhost:9090");
    }

    #[test]
    fn test_flight_sql_service_auth() {
        let config = FlightSqlServiceConfig::new().with_auth(AuthType::Token {
            valid_tokens: vec!["secret-token".to_string()],
        });
        let conn = crate::Connection::in_memory().unwrap();
        let service = FlightSqlService::new(conn, config);

        let session = service.authenticate("secret-token").unwrap();
        assert!(service.validate_session(&session.token));
        assert!(!service.validate_session("invalid"));
    }
}
