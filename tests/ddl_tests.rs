//! DDL and table management integration tests.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use blaze::{Connection, ConnectionConfig};

mod common;
use common::create_test_connection;

#[test]
fn test_create_table() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE test (id INT, name VARCHAR)")
        .unwrap();
    let tables = conn.list_tables();
    assert!(tables.contains(&"test".to_string()));
}

#[test]
fn test_drop_table() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE test (id INT)").unwrap();
    conn.execute("DROP TABLE test").unwrap();
    let tables = conn.list_tables();
    assert!(!tables.contains(&"test".to_string()));
}

#[test]
fn test_drop_table_if_exists() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.execute("DROP TABLE IF EXISTS nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_multiple_tables() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE t1 (id INT)").unwrap();
    conn.execute("CREATE TABLE t2 (id INT)").unwrap();
    conn.execute("CREATE TABLE t3 (id INT)").unwrap();
    let tables = conn.list_tables();
    assert_eq!(tables.len(), 3);
}

#[test]
fn test_default_connection() {
    let conn = Connection::in_memory().unwrap();
    assert!(conn.list_tables().is_empty());
}

#[test]
fn test_config_batch_size() {
    let config = ConnectionConfig::new().with_batch_size(4096);
    let conn = Connection::with_config(config).unwrap();
    assert!(conn.list_tables().is_empty());
}

#[test]
fn test_config_memory_limit() {
    let config = ConnectionConfig::new().with_memory_limit(100 * 1024 * 1024);
    let conn = Connection::with_config(config).unwrap();
    assert!(conn.list_tables().is_empty());
}

#[test]
fn test_config_combined() {
    let config = ConnectionConfig::new()
        .with_batch_size(2048)
        .with_memory_limit(50 * 1024 * 1024);
    let conn = Connection::with_config(config).unwrap();
    assert!(conn.list_tables().is_empty());
}

#[test]
fn test_register_batches() {
    let conn = Connection::in_memory().unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

    conn.register_batches("test", vec![batch]).unwrap();
    assert!(conn.list_tables().contains(&"test".to_string()));
}

#[test]
fn test_table_schema() {
    let conn = create_test_connection();
    let schema = conn.table_schema("users");
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.len(), 4);
}

#[test]
fn test_table_schema_nonexistent() {
    let conn = Connection::in_memory().unwrap();
    let schema = conn.table_schema("nonexistent");
    assert!(schema.is_none());
}

#[test]
fn test_empty_table() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE empty (id INT)").unwrap();
    let results = conn.query("SELECT * FROM empty").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[test]
fn test_empty_table_with_limit() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE empty (id INT)").unwrap();
    let results = conn.query("SELECT * FROM empty LIMIT 10").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[test]
fn test_create_table_if_not_exists_no_error() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE t1 (id BIGINT)").unwrap();
    let result = conn.execute("CREATE TABLE IF NOT EXISTS t1 (id BIGINT)");
    assert!(result.is_ok());
}

#[test]
fn test_drop_table_if_exists_no_error() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.execute("DROP TABLE IF EXISTS nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_register_nonexistent_csv() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.register_csv("test", "data/this_does_not_exist.csv");
    assert!(result.is_err());
}
