//! Zero-copy IPC protocol for cross-process communication.
//!
//! Implements an Arrow IPC-based protocol for serving Blaze queries
//! over Unix domain sockets or TCP, enabling polyglot access with
//! minimal serialization overhead.
//!
//! # Protocol
//!
//! Request: 4-byte length prefix + JSON message body
//! Response: Arrow IPC stream (schema + record batches)

use std::io::{Read, Write};

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use crate::Connection;

/// IPC message types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum IpcRequest {
    /// Execute a SQL query and return results as Arrow IPC
    #[serde(rename = "query")]
    Query { sql: String },
    /// Execute a SQL statement (DDL/DML) and return affected row count
    #[serde(rename = "execute")]
    Execute { sql: String },
    /// List available tables
    #[serde(rename = "list_tables")]
    ListTables,
    /// Get schema of a table
    #[serde(rename = "get_schema")]
    GetSchema { table: String },
    /// Ping/health check
    #[serde(rename = "ping")]
    Ping,
    /// Shutdown the server
    #[serde(rename = "shutdown")]
    Shutdown,
}

/// Response metadata sent before Arrow IPC data.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IpcResponseMeta {
    pub status: IpcStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
    /// Number of Arrow IPC bytes that follow this metadata
    pub data_bytes: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum IpcStatus {
    #[serde(rename = "ok")]
    Ok,
    #[serde(rename = "error")]
    Error,
}

/// Encode RecordBatches to Arrow IPC format in memory.
pub fn encode_batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
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

/// Decode RecordBatches from Arrow IPC format.
pub fn decode_batches_from_ipc(data: &[u8]) -> Result<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = std::io::Cursor::new(data);
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }
    Ok(batches)
}

/// Write a length-prefixed message to a stream.
pub fn write_message(writer: &mut impl Write, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(data)?;
    writer.flush()?;
    Ok(())
}

/// Read a length-prefixed message from a stream.
pub fn read_message(reader: &mut impl Read) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 100 * 1024 * 1024 {
        return Err(BlazeError::invalid_argument(format!(
            "IPC message too large: {} bytes",
            len
        )));
    }

    let mut data = vec![0u8; len];
    reader.read_exact(&mut data)?;
    Ok(data)
}

/// Handle a single IPC request and produce a response.
pub fn handle_request(conn: &Connection, request: &IpcRequest) -> (IpcResponseMeta, Vec<u8>) {
    match request {
        IpcRequest::Query { sql } => match conn.query(sql) {
            Ok(batches) => {
                let ipc_data = encode_batches_to_ipc(&batches).unwrap_or_default();
                (
                    IpcResponseMeta {
                        status: IpcStatus::Ok,
                        error: None,
                        rows_affected: None,
                        tables: None,
                        data_bytes: ipc_data.len(),
                    },
                    ipc_data,
                )
            }
            Err(e) => (
                IpcResponseMeta {
                    status: IpcStatus::Error,
                    error: Some(e.to_string()),
                    rows_affected: None,
                    tables: None,
                    data_bytes: 0,
                },
                Vec::new(),
            ),
        },
        IpcRequest::Execute { sql } => match conn.execute(sql) {
            Ok(rows) => (
                IpcResponseMeta {
                    status: IpcStatus::Ok,
                    error: None,
                    rows_affected: Some(rows),
                    tables: None,
                    data_bytes: 0,
                },
                Vec::new(),
            ),
            Err(e) => (
                IpcResponseMeta {
                    status: IpcStatus::Error,
                    error: Some(e.to_string()),
                    rows_affected: None,
                    tables: None,
                    data_bytes: 0,
                },
                Vec::new(),
            ),
        },
        IpcRequest::ListTables => {
            let tables = conn.list_tables();
            (
                IpcResponseMeta {
                    status: IpcStatus::Ok,
                    error: None,
                    rows_affected: None,
                    tables: Some(tables),
                    data_bytes: 0,
                },
                Vec::new(),
            )
        }
        IpcRequest::GetSchema { table } => match conn.table_schema(table) {
            Some(schema) => {
                let field_names: Vec<String> = schema
                    .fields()
                    .iter()
                    .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                    .collect();
                (
                    IpcResponseMeta {
                        status: IpcStatus::Ok,
                        error: None,
                        rows_affected: None,
                        tables: Some(field_names),
                        data_bytes: 0,
                    },
                    Vec::new(),
                )
            }
            None => (
                IpcResponseMeta {
                    status: IpcStatus::Error,
                    error: Some(format!("Table '{}' not found", table)),
                    rows_affected: None,
                    tables: None,
                    data_bytes: 0,
                },
                Vec::new(),
            ),
        },
        IpcRequest::Ping => (
            IpcResponseMeta {
                status: IpcStatus::Ok,
                error: None,
                rows_affected: None,
                tables: None,
                data_bytes: 0,
            },
            Vec::new(),
        ),
        IpcRequest::Shutdown => (
            IpcResponseMeta {
                status: IpcStatus::Ok,
                error: None,
                rows_affected: None,
                tables: None,
                data_bytes: 0,
            },
            Vec::new(),
        ),
    }
}

/// Process a single connection stream (e.g., from a Unix domain socket).
/// Reads requests, handles them, and writes responses until the stream closes.
pub fn serve_connection(conn: &Connection, mut stream: impl Read + Write) -> Result<()> {
    loop {
        // Read request
        let request_data = match read_message(&mut stream) {
            Ok(data) => data,
            Err(_) => break, // Connection closed
        };

        let request: IpcRequest = serde_json::from_slice(&request_data).map_err(|e| {
            BlazeError::invalid_argument(format!("Invalid IPC request: {}", e))
        })?;

        let is_shutdown = matches!(request, IpcRequest::Shutdown);

        // Handle request
        let (meta, ipc_data) = handle_request(conn, &request);

        // Write response metadata
        let meta_json = serde_json::to_vec(&meta).map_err(|e| {
            BlazeError::internal(format!("Failed to serialize response: {}", e))
        })?;
        write_message(&mut stream, &meta_json)?;

        // Write IPC data if present
        if !ipc_data.is_empty() {
            write_message(&mut stream, &ipc_data)?;
        }

        if is_shutdown {
            break;
        }
    }
    Ok(())
}

/// IPC client for connecting to a Blaze IPC server.
pub struct IpcClient<S: Read + Write> {
    stream: S,
}

impl<S: Read + Write> IpcClient<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    /// Send a request and receive the response.
    pub fn request(&mut self, req: &IpcRequest) -> Result<(IpcResponseMeta, Vec<RecordBatch>)> {
        // Send request
        let req_data = serde_json::to_vec(req).map_err(|e| {
            BlazeError::internal(format!("Failed to serialize request: {}", e))
        })?;
        write_message(&mut self.stream, &req_data)?;

        // Read response metadata
        let meta_data = read_message(&mut self.stream)?;
        let meta: IpcResponseMeta = serde_json::from_slice(&meta_data).map_err(|e| {
            BlazeError::internal(format!("Failed to deserialize response: {}", e))
        })?;

        // Read IPC data if present
        let batches = if meta.data_bytes > 0 {
            let ipc_data = read_message(&mut self.stream)?;
            decode_batches_from_ipc(&ipc_data)?
        } else {
            Vec::new()
        };

        Ok((meta, batches))
    }

    /// Execute a query and return record batches.
    pub fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let (meta, batches) = self.request(&IpcRequest::Query {
            sql: sql.to_string(),
        })?;

        if let IpcStatus::Error = meta.status {
            return Err(BlazeError::execution(
                meta.error.unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        Ok(batches)
    }

    /// Execute a statement and return rows affected.
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (meta, _) = self.request(&IpcRequest::Execute {
            sql: sql.to_string(),
        })?;

        if let IpcStatus::Error = meta.status {
            return Err(BlazeError::execution(
                meta.error.unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        Ok(meta.rows_affected.unwrap_or(0))
    }

    /// Ping the server.
    pub fn ping(&mut self) -> Result<bool> {
        let (meta, _) = self.request(&IpcRequest::Ping)?;
        Ok(matches!(meta.status, IpcStatus::Ok))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_encode_decode_ipc() {
        let batch = make_test_batch();
        let encoded = encode_batches_to_ipc(&[batch.clone()]).unwrap();
        assert!(!encoded.is_empty());

        let decoded = decode_batches_from_ipc(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 3);
        assert_eq!(decoded[0].num_columns(), 2);
    }

    #[test]
    fn test_encode_decode_empty() {
        let encoded = encode_batches_to_ipc(&[]).unwrap();
        assert!(encoded.is_empty());

        let decoded = decode_batches_from_ipc(&[]).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_write_read_message() {
        let data = b"hello world";
        let mut buf = Vec::new();
        write_message(&mut buf, data).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let result = read_message(&mut cursor).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_handle_query() {
        let conn = Connection::in_memory().unwrap();
        let req = IpcRequest::Query {
            sql: "SELECT 1 + 1 AS result".to_string(),
        };
        let (meta, data) = handle_request(&conn, &req);
        assert!(matches!(meta.status, IpcStatus::Ok));
        assert!(meta.data_bytes > 0);

        let batches = decode_batches_from_ipc(&data).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_handle_execute() {
        let conn = Connection::in_memory().unwrap();

        let req = IpcRequest::Execute {
            sql: "CREATE TABLE t (id INT)".to_string(),
        };
        let (meta, _) = handle_request(&conn, &req);
        assert!(matches!(meta.status, IpcStatus::Ok));
    }

    #[test]
    fn test_handle_ping() {
        let conn = Connection::in_memory().unwrap();
        let (meta, _) = handle_request(&conn, &IpcRequest::Ping);
        assert!(matches!(meta.status, IpcStatus::Ok));
    }

    #[test]
    fn test_handle_list_tables() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE users (id INT)").unwrap();

        let (meta, _) = handle_request(&conn, &IpcRequest::ListTables);
        assert!(matches!(meta.status, IpcStatus::Ok));
        assert!(meta.tables.is_some());
    }

    #[test]
    fn test_handle_invalid_query() {
        let conn = Connection::in_memory().unwrap();
        let req = IpcRequest::Query {
            sql: "INVALID SQL THAT SHOULD FAIL".to_string(),
        };
        let (meta, _) = handle_request(&conn, &req);
        assert!(matches!(meta.status, IpcStatus::Error));
        assert!(meta.error.is_some());
    }

    #[test]
    fn test_client_server_roundtrip() {
        use std::io::Cursor;

        let conn = Connection::in_memory().unwrap();

        // Simulate a client-server roundtrip via in-memory buffer
        let req = IpcRequest::Query {
            sql: "SELECT 42 AS answer".to_string(),
        };
        let (meta, data) = handle_request(&conn, &req);

        // Simulate writing server response to buffer
        let mut response_buf = Vec::new();
        let meta_json = serde_json::to_vec(&meta).unwrap();
        write_message(&mut response_buf, &meta_json).unwrap();
        if !data.is_empty() {
            write_message(&mut response_buf, &data).unwrap();
        }

        // Read back as client
        let mut cursor = Cursor::new(response_buf);
        let meta_data = read_message(&mut cursor).unwrap();
        let response_meta: IpcResponseMeta = serde_json::from_slice(&meta_data).unwrap();
        assert!(matches!(response_meta.status, IpcStatus::Ok));

        if response_meta.data_bytes > 0 {
            let ipc_data = read_message(&mut cursor).unwrap();
            let batches = decode_batches_from_ipc(&ipc_data).unwrap();
            assert!(!batches.is_empty());
        }
    }

    #[test]
    fn test_request_serialization() {
        let req = IpcRequest::Query {
            sql: "SELECT 1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("query"));
        assert!(json.contains("SELECT 1"));

        let deserialized: IpcRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcRequest::Query { sql } => assert_eq!(sql, "SELECT 1"),
            _ => panic!("Wrong variant"),
        }
    }
}
