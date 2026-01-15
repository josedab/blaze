//! C Foreign Function Interface (FFI) for Blaze Query Engine
//!
//! This module provides C-compatible bindings for the Blaze query engine,
//! enabling integration with C, C++, and other languages via FFI.
//!
//! # Usage from C
//!
//! ```c
//! #include "blaze.h"
//!
//! int main() {
//!     // Create a connection
//!     BlazeConnection* conn = blaze_connection_new();
//!     if (conn == NULL) {
//!         printf("Failed to create connection\n");
//!         return 1;
//!     }
//!
//!     // Execute a query
//!     BlazeResult* result = blaze_query(conn, "SELECT 1 + 1 AS result");
//!     if (result == NULL) {
//!         printf("Query failed: %s\n", blaze_last_error(conn));
//!         blaze_connection_free(conn);
//!         return 1;
//!     }
//!
//!     // Get results
//!     int64_t row_count = blaze_result_row_count(result);
//!     printf("Rows: %lld\n", row_count);
//!
//!     // Cleanup
//!     blaze_result_free(result);
//!     blaze_connection_free(conn);
//!     return 0;
//! }
//! ```
//!
//! # Memory Management
//!
//! All objects returned from the FFI must be freed by the caller using
//! the corresponding `*_free` function. Failure to do so will result
//! in memory leaks.
//!
//! # Thread Safety
//!
//! - `BlazeConnection` is NOT thread-safe. Use separate connections per thread.
//! - `BlazeResult` can be read from multiple threads but not modified.

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::sync::Mutex;

use crate::error::BlazeError;
use crate::Connection;

/// Opaque handle to a Blaze database connection.
///
/// This type is used in the C API to represent a connection.
/// All operations on this handle must use the `blaze_*` functions.
#[repr(C)]
pub struct BlazeConnection {
    inner: Connection,
    last_error: Mutex<Option<String>>,
}

/// Opaque handle to a query result.
///
/// Contains the result of a query execution including rows and schema.
#[repr(C)]
pub struct BlazeResult {
    /// JSON-serialized result data
    data: String,
    /// Number of rows
    row_count: i64,
    /// Number of columns
    column_count: i64,
    /// Schema JSON
    schema: Option<String>,
}

/// Result status codes for FFI operations.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlazeStatus {
    /// Operation succeeded
    Ok = 0,
    /// Invalid argument
    InvalidArgument = 1,
    /// SQL syntax error
    SyntaxError = 2,
    /// Query execution error
    ExecutionError = 3,
    /// Table not found
    TableNotFound = 4,
    /// Memory allocation failed
    OutOfMemory = 5,
    /// Internal error
    InternalError = 6,
    /// Null pointer passed
    NullPointer = 7,
}

/// Column information for FFI.
#[repr(C)]
pub struct BlazeColumn {
    /// Column name (null-terminated string)
    pub name: *mut c_char,
    /// Column type (null-terminated string)
    pub data_type: *mut c_char,
    /// Whether column is nullable
    pub nullable: c_int,
}

// ============================================================================
// Connection Management
// ============================================================================

/// Create a new in-memory database connection.
///
/// # Returns
/// - Pointer to a new `BlazeConnection` on success
/// - NULL on failure
///
/// # Safety
/// The returned pointer must be freed with `blaze_connection_free`.
#[no_mangle]
pub extern "C" fn blaze_connection_new() -> *mut BlazeConnection {
    match Connection::in_memory() {
        Ok(conn) => {
            let boxed = Box::new(BlazeConnection {
                inner: conn,
                last_error: Mutex::new(None),
            });
            Box::into_raw(boxed)
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Free a database connection.
///
/// # Safety
/// - `conn` must be a valid pointer returned by `blaze_connection_new`
/// - After calling this function, `conn` is invalid and must not be used
#[no_mangle]
pub unsafe extern "C" fn blaze_connection_free(conn: *mut BlazeConnection) {
    if !conn.is_null() {
        drop(Box::from_raw(conn));
    }
}

/// Get the last error message from a connection.
///
/// # Returns
/// - Pointer to a null-terminated string on error
/// - NULL if no error
///
/// # Safety
/// - `conn` must be a valid pointer
/// - The returned string is valid until the next operation on `conn`
#[no_mangle]
pub unsafe extern "C" fn blaze_last_error(conn: *const BlazeConnection) -> *const c_char {
    if conn.is_null() {
        return ptr::null();
    }

    let conn = &*conn;
    match conn.last_error.lock() {
        Ok(guard) => match &*guard {
            Some(err) => {
                // This is a temporary string - in production, we'd need to
                // manage the lifetime more carefully
                match CString::new(err.as_str()) {
                    Ok(cstr) => cstr.into_raw(),
                    Err(_) => ptr::null(),
                }
            }
            None => ptr::null(),
        },
        Err(_) => ptr::null(),
    }
}

/// Clear the last error on a connection.
///
/// # Safety
/// `conn` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn blaze_clear_error(conn: *mut BlazeConnection) {
    if conn.is_null() {
        return;
    }

    let conn = &*conn;
    if let Ok(mut guard) = conn.last_error.lock() {
        *guard = None;
    }
}

// ============================================================================
// Query Execution
// ============================================================================

/// Execute a SQL query and return results.
///
/// # Arguments
/// - `conn`: Valid connection pointer
/// - `sql`: Null-terminated SQL query string
///
/// # Returns
/// - Pointer to a new `BlazeResult` on success
/// - NULL on failure (check `blaze_last_error`)
///
/// # Safety
/// - `conn` must be a valid pointer
/// - `sql` must be a valid null-terminated string
/// - The returned pointer must be freed with `blaze_result_free`
#[no_mangle]
pub unsafe extern "C" fn blaze_query(
    conn: *mut BlazeConnection,
    sql: *const c_char,
) -> *mut BlazeResult {
    if conn.is_null() || sql.is_null() {
        return ptr::null_mut();
    }

    let conn = &mut *conn;
    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(conn, "Invalid UTF-8 in SQL string");
            return ptr::null_mut();
        }
    };

    match conn.inner.query(sql_str) {
        Ok(batches) => {
            // Serialize to JSON
            let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
            let column_count = batches.first().map(|b| b.num_columns() as i64).unwrap_or(0);

            // Serialize batches to JSON
            let data = match serialize_batches_to_json(&batches) {
                Ok(json) => json,
                Err(e) => {
                    set_error(conn, &e.to_string());
                    return ptr::null_mut();
                }
            };

            // Get schema
            let schema = batches.first().map(|b| {
                let fields: Vec<String> = b
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| {
                        format!(
                            "{{\"name\":\"{}\",\"type\":\"{:?}\",\"nullable\":{}}}",
                            f.name(),
                            f.data_type(),
                            f.is_nullable()
                        )
                    })
                    .collect();
                format!("[{}]", fields.join(","))
            });

            let result = Box::new(BlazeResult {
                data,
                row_count,
                column_count,
                schema,
            });

            Box::into_raw(result)
        }
        Err(e) => {
            set_error(conn, &e.to_string());
            ptr::null_mut()
        }
    }
}

/// Execute a SQL statement (DDL like CREATE TABLE, DROP TABLE).
///
/// # Arguments
/// - `conn`: Valid connection pointer
/// - `sql`: Null-terminated SQL statement string
///
/// # Returns
/// - Number of rows affected on success (>= 0)
/// - Negative value on error (check `blaze_last_error`)
///
/// # Safety
/// - `conn` must be a valid pointer
/// - `sql` must be a valid null-terminated string
#[no_mangle]
pub unsafe extern "C" fn blaze_execute(conn: *mut BlazeConnection, sql: *const c_char) -> i64 {
    if conn.is_null() || sql.is_null() {
        return -1;
    }

    let conn = &mut *conn;
    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(conn, "Invalid UTF-8 in SQL string");
            return -1;
        }
    };

    match conn.inner.execute(sql_str) {
        Ok(count) => count as i64,
        Err(e) => {
            set_error(conn, &e.to_string());
            -1
        }
    }
}

// ============================================================================
// Result Access
// ============================================================================

/// Get the number of rows in a result.
///
/// # Safety
/// `result` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn blaze_result_row_count(result: *const BlazeResult) -> i64 {
    if result.is_null() {
        return 0;
    }
    (*result).row_count
}

/// Get the number of columns in a result.
///
/// # Safety
/// `result` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn blaze_result_column_count(result: *const BlazeResult) -> i64 {
    if result.is_null() {
        return 0;
    }
    (*result).column_count
}

/// Get the result data as a JSON string.
///
/// # Returns
/// - Pointer to a null-terminated JSON string
/// - NULL if result is invalid
///
/// # Safety
/// - `result` must be a valid pointer
/// - The returned string is valid until `blaze_result_free` is called
#[no_mangle]
pub unsafe extern "C" fn blaze_result_to_json(result: *const BlazeResult) -> *const c_char {
    if result.is_null() {
        return ptr::null();
    }

    match CString::new((*result).data.as_str()) {
        Ok(cstr) => cstr.into_raw(),
        Err(_) => ptr::null(),
    }
}

/// Get the result schema as a JSON string.
///
/// # Returns
/// - Pointer to a null-terminated JSON string
/// - NULL if no schema or result is invalid
///
/// # Safety
/// - `result` must be a valid pointer
/// - The returned string is valid until `blaze_result_free` is called
#[no_mangle]
pub unsafe extern "C" fn blaze_result_schema(result: *const BlazeResult) -> *const c_char {
    if result.is_null() {
        return ptr::null();
    }

    match &(*result).schema {
        Some(schema) => match CString::new(schema.as_str()) {
            Ok(cstr) => cstr.into_raw(),
            Err(_) => ptr::null(),
        },
        None => ptr::null(),
    }
}

/// Free a result object.
///
/// # Safety
/// - `result` must be a valid pointer returned by `blaze_query`
/// - After calling this function, `result` is invalid
#[no_mangle]
pub unsafe extern "C" fn blaze_result_free(result: *mut BlazeResult) {
    if !result.is_null() {
        drop(Box::from_raw(result));
    }
}

/// Free a string returned by the FFI.
///
/// # Safety
/// - `s` must be a valid pointer returned by a `blaze_*` function
#[no_mangle]
pub unsafe extern "C" fn blaze_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

// ============================================================================
// Table Management
// ============================================================================

/// Register JSON data as a table.
///
/// # Arguments
/// - `conn`: Valid connection pointer
/// - `table_name`: Null-terminated table name
/// - `json_data`: Null-terminated JSON array string
///
/// # Returns
/// - Number of rows registered on success (>= 0)
/// - Negative value on error
///
/// # Safety
/// All pointers must be valid null-terminated strings
#[no_mangle]
pub unsafe extern "C" fn blaze_register_json(
    conn: *mut BlazeConnection,
    table_name: *const c_char,
    json_data: *const c_char,
) -> i64 {
    if conn.is_null() || table_name.is_null() || json_data.is_null() {
        return -1;
    }

    let conn = &mut *conn;

    let table_str = match CStr::from_ptr(table_name).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(conn, "Invalid UTF-8 in table name");
            return -1;
        }
    };

    let json_str = match CStr::from_ptr(json_data).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(conn, "Invalid UTF-8 in JSON data");
            return -1;
        }
    };

    // Parse JSON and register
    match parse_json_to_batches(json_str) {
        Ok(batches) => {
            let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
            match conn.inner.register_batches(table_str, batches) {
                Ok(_) => row_count,
                Err(e) => {
                    set_error(conn, &e.to_string());
                    -1
                }
            }
        }
        Err(e) => {
            set_error(conn, &e.to_string());
            -1
        }
    }
}

/// Drop a table from the connection.
///
/// # Arguments
/// - `conn`: Valid connection pointer
/// - `table_name`: Null-terminated table name
///
/// # Returns
/// - 0 on success
/// - Non-zero on error
///
/// # Safety
/// All pointers must be valid
#[no_mangle]
pub unsafe extern "C" fn blaze_drop_table(
    conn: *mut BlazeConnection,
    table_name: *const c_char,
) -> c_int {
    if conn.is_null() || table_name.is_null() {
        return -1;
    }

    let conn = &mut *conn;

    let table_str = match CStr::from_ptr(table_name).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(conn, "Invalid UTF-8 in table name");
            return -1;
        }
    };

    match conn.inner.deregister_table(table_str) {
        Ok(_) => 0,
        Err(e) => {
            set_error(conn, &e.to_string());
            -1
        }
    }
}

/// List all tables in the connection.
///
/// # Returns
/// - JSON array of table names
/// - NULL on error
///
/// # Safety
/// - `conn` must be a valid pointer
/// - The returned string must be freed with `blaze_string_free`
#[no_mangle]
pub unsafe extern "C" fn blaze_list_tables(conn: *const BlazeConnection) -> *mut c_char {
    if conn.is_null() {
        return ptr::null_mut();
    }

    let conn = &*conn;
    let tables = conn.inner.list_tables();

    let json = format!(
        "[{}]",
        tables
            .iter()
            .map(|t| format!("\"{}\"", t))
            .collect::<Vec<_>>()
            .join(",")
    );

    match CString::new(json) {
        Ok(cstr) => cstr.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

// ============================================================================
// Version Info
// ============================================================================

/// Get the Blaze version string.
///
/// # Returns
/// Null-terminated version string (e.g., "0.1.0")
///
/// # Safety
/// The returned string is static and does not need to be freed.
#[no_mangle]
pub extern "C" fn blaze_version() -> *const c_char {
    static VERSION: &[u8] = b"0.1.0\0";
    VERSION.as_ptr() as *const c_char
}

// ============================================================================
// Helper Functions
// ============================================================================

fn set_error(conn: &BlazeConnection, msg: &str) {
    if let Ok(mut guard) = conn.last_error.lock() {
        *guard = Some(msg.to_string());
    }
}

fn serialize_batches_to_json(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<String, BlazeError> {
    use std::io::Cursor;

    if batches.is_empty() {
        return Ok("[]".to_string());
    }

    let mut buf = Cursor::new(Vec::new());
    {
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        for batch in batches {
            writer.write(batch).map_err(|e| {
                BlazeError::execution(format!("JSON serialization failed: {}", e))
            })?;
        }
        writer.finish().map_err(|e| {
            BlazeError::execution(format!("JSON serialization failed: {}", e))
        })?;
    }

    String::from_utf8(buf.into_inner())
        .map_err(|e| BlazeError::execution(format!("Invalid UTF-8: {}", e)))
}

fn parse_json_to_batches(
    json: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>, BlazeError> {
    use arrow_json::ReaderBuilder;
    use std::io::Cursor;

    let cursor = Cursor::new(json.as_bytes());
    let reader = ReaderBuilder::new(arrow::datatypes::SchemaRef::new(
        arrow::datatypes::Schema::empty(),
    ))
    .with_batch_size(1024)
    .build(cursor)
    .map_err(|e| BlazeError::execution(format!("Failed to parse JSON: {}", e)))?;

    let batches: Result<Vec<_>, _> = reader.collect();
    batches.map_err(|e| BlazeError::execution(format!("Failed to read JSON batches: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_lifecycle() {
        unsafe {
            let conn = blaze_connection_new();
            assert!(!conn.is_null());

            blaze_connection_free(conn);
        }
    }

    #[test]
    fn test_simple_query() {
        unsafe {
            let conn = blaze_connection_new();
            assert!(!conn.is_null());

            let sql = CString::new("SELECT 1 + 1 AS result").unwrap();
            let result = blaze_query(conn, sql.as_ptr());
            assert!(!result.is_null());

            let row_count = blaze_result_row_count(result);
            assert_eq!(row_count, 1);

            blaze_result_free(result);
            blaze_connection_free(conn);
        }
    }

    #[test]
    fn test_execute_ddl() {
        unsafe {
            let conn = blaze_connection_new();
            assert!(!conn.is_null());

            let sql = CString::new("CREATE TABLE test (id INT, name VARCHAR)").unwrap();
            let result = blaze_execute(conn, sql.as_ptr());
            assert!(result >= 0);

            blaze_connection_free(conn);
        }
    }

    #[test]
    fn test_list_tables() {
        unsafe {
            let conn = blaze_connection_new();
            assert!(!conn.is_null());

            let sql = CString::new("CREATE TABLE my_table (id INT)").unwrap();
            blaze_execute(conn, sql.as_ptr());

            let tables = blaze_list_tables(conn);
            assert!(!tables.is_null());

            let tables_str = CStr::from_ptr(tables).to_str().unwrap();
            assert!(tables_str.contains("my_table"));

            blaze_string_free(tables);
            blaze_connection_free(conn);
        }
    }

    #[test]
    fn test_version() {
        let version = blaze_version();
        assert!(!version.is_null());

        unsafe {
            let version_str = CStr::from_ptr(version).to_str().unwrap();
            assert_eq!(version_str, "0.1.0");
        }
    }
}
