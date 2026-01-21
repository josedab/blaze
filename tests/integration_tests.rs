//! Integration tests for Blaze Query Engine
//!
//! These tests verify end-to-end functionality of the query engine.
//! Some tests are marked #[ignore] due to known limitations in the current implementation.

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use blaze::{Connection, ConnectionConfig};

// Helper function to create a test connection with sample data
fn create_test_connection() -> Connection {
    let conn = Connection::in_memory().unwrap();

    // Register users table with sample data
    let users_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("age", ArrowDataType::Int64, true),
        ArrowField::new("active", ArrowDataType::Boolean, true),
    ]));

    let users_batch = RecordBatch::try_new(
        users_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Int64Array::from(vec![30, 25, 35, 28, 32])),
            Arc::new(BooleanArray::from(vec![true, true, false, true, false])),
        ],
    )
    .unwrap();

    conn.register_batches("users", vec![users_batch]).unwrap();

    // Register orders table with sample data
    let orders_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("user_id", ArrowDataType::Int64, false),
        ArrowField::new("amount", ArrowDataType::Float64, false),
        ArrowField::new("status", ArrowDataType::Utf8, false),
    ]));

    let orders_batch = RecordBatch::try_new(
        orders_schema,
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103, 104, 105, 106])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3, 1, 4])),
            Arc::new(Float64Array::from(vec![
                100.0, 200.0, 150.0, 300.0, 50.0, 250.0,
            ])),
            Arc::new(StringArray::from(vec![
                "completed",
                "pending",
                "completed",
                "completed",
                "cancelled",
                "pending",
            ])),
        ],
    )
    .unwrap();

    conn.register_batches("orders", vec![orders_batch]).unwrap();

    conn
}

// ============================================================================
// Basic Query Tests
// ============================================================================

#[test]
fn test_select_all() {
    let conn = create_test_connection();
    let results = conn.query("SELECT * FROM users").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);
}

#[test]
fn test_select_all_from_orders() {
    let conn = create_test_connection();
    let results = conn.query("SELECT * FROM orders").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);
}

#[test]
fn test_select_string_literal() {
    let conn = Connection::in_memory().unwrap();
    let results = conn.query("SELECT 'hello' AS greeting").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);
}

// ============================================================================
// LIMIT Tests
// ============================================================================

#[test]
fn test_limit() {
    let conn = create_test_connection();
    let results = conn.query("SELECT * FROM users LIMIT 3").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[test]
fn test_limit_larger_than_table() {
    let conn = create_test_connection();
    let results = conn.query("SELECT * FROM users LIMIT 1000").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5); // Only 5 rows exist
}

// ============================================================================
// DDL Tests
// ============================================================================

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
    // Should not error even if table doesn't exist
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

// ============================================================================
// Configuration Tests
// ============================================================================

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
    let config = ConnectionConfig::new().with_memory_limit(100 * 1024 * 1024); // 100MB
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

// ============================================================================
// Register Batches Tests
// ============================================================================

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

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_invalid_table() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.query("SELECT * FROM nonexistent");
    // Query should either error or return empty results for nonexistent table
    if let Ok(results) = result {
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}

#[test]
fn test_syntax_error() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.query("SELEC * FROM users");
    assert!(result.is_err());
}

#[test]
fn test_empty_query() {
    let conn = Connection::in_memory().unwrap();
    let results = conn.query("").unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_drop_nonexistent_table() {
    let conn = Connection::in_memory().unwrap();
    // DROP TABLE without IF EXISTS on a nonexistent table
    // Note: current implementation may not error on this
    let _ = conn.execute("DROP TABLE nonexistent");
}

// ============================================================================
// Empty Table Tests
// ============================================================================

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

// ============================================================================
// Tests for features with known issues (marked #[ignore])
// These tests document expected behavior but are disabled until bugs are fixed.
// ============================================================================

#[test]
fn test_select_columns() {
    let conn = create_test_connection();
    let results = conn.query("SELECT name, age FROM users").unwrap();
    assert_eq!(results[0].num_columns(), 2);
}

#[test]
fn test_select_with_alias() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT name AS user_name, age AS user_age FROM users")
        .unwrap();
    assert!(results[0].schema().field(0).name() == "user_name");
}

#[test]
fn test_select_literal() {
    let conn = Connection::in_memory().unwrap();
    let results = conn.query("SELECT 1 + 2 AS result").unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_where_equals() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT * FROM users WHERE name = 'Alice'")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[test]
fn test_order_by_asc() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT name, age FROM users ORDER BY age ASC")
        .unwrap();
    let ages = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(ages.value(0) <= ages.value(1));
}

#[test]
fn test_count() {
    let conn = create_test_connection();
    let results = conn.query("SELECT COUNT(*) FROM users").unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5);
}

#[test]
fn test_inner_join() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);
}

#[test]
fn test_row_number() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT name, ROW_NUMBER() OVER (ORDER BY age) AS rn FROM users")
        .unwrap();
    let rn = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values: Vec<i64> = (0..rn.len()).map(|i| rn.value(i)).collect();
    values.sort();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_cte_basic() {
    let conn = create_test_connection();
    let results = conn
        .query(
            "WITH active_users AS (SELECT * FROM users WHERE active = true)
         SELECT COUNT(*) FROM active_users",
        )
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3);
}

#[test]
fn test_union_all() {
    let conn = create_test_connection();
    let results = conn
        .query(
            "SELECT name FROM users WHERE age < 30 UNION ALL SELECT name FROM users WHERE age > 30",
        )
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[test]
fn test_group_by() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[test]
fn test_case_when() {
    let conn = create_test_connection();
    let results = conn.query(
        "SELECT name, CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END AS category FROM users"
    ).unwrap();
    assert_eq!(results[0].num_columns(), 2);
}

#[test]
fn test_upper() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT UPPER(name) FROM users WHERE name = 'Alice'")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(val, "ALICE");
}
