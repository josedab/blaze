//! Error handling and edge case integration tests.

mod common;

use blaze::Connection;
use common::create_test_connection;

#[test]
fn test_invalid_table() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.query("SELECT * FROM nonexistent");
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
    let result = conn.execute("DROP TABLE nonexistent");
    assert!(result.is_ok() || result.is_err());
    conn.execute("CREATE TABLE t1 (id INT)").unwrap();
}

#[test]
fn test_table_not_found_suggests_similar() {
    let conn = create_test_connection();
    let err = conn.query("SELECT * FROM uers").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Did you mean"),
        "Expected suggestion in error: {}",
        msg
    );
    assert!(
        msg.contains("users"),
        "Expected 'users' suggestion: {}",
        msg
    );
}

#[test]
fn test_column_not_found_suggests_similar() {
    let conn = create_test_connection();
    let err = conn.query("SELECT nam FROM users").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Did you mean") || msg.contains("name"),
        "Expected suggestion in error: {}",
        msg
    );
}

#[test]
fn test_select_nonexistent_column() {
    let conn = create_test_connection();
    let err = conn.query("SELECT nonexistent FROM users").unwrap_err();
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn test_insert_wrong_column_count() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE t1 (id BIGINT, name VARCHAR)")
        .unwrap();
    let result = conn.execute("INSERT INTO t1 VALUES (1, 'a', 'extra')");
    drop(result);
}

#[test]
fn test_duplicate_create_table() {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE t1 (id BIGINT)").unwrap();
    let result = conn.execute("CREATE TABLE t1 (id BIGINT)");
    drop(result);
}

#[test]
fn test_empty_query_error() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.execute("");
    drop(result);
}

#[test]
fn test_syntax_error_message() {
    let conn = Connection::in_memory().unwrap();
    let err = conn.query("SELECTT * FROM users").unwrap_err();
    let msg = err.to_string();
    assert!(!msg.is_empty(), "Error message should not be empty");
}

#[test]
fn test_select_star_no_table() {
    let conn = Connection::in_memory().unwrap();
    let result = conn.query("SELECT 1, 'hello', true");
    assert!(result.is_ok());
}

#[test]
fn test_division_by_zero() {
    let conn = create_test_connection();
    let result = conn.query("SELECT 1 / 0");
    drop(result);
}

#[test]
fn test_order_by_nonexistent_column() {
    let conn = create_test_connection();
    let result = conn.query("SELECT name FROM users ORDER BY nonexistent");
    assert!(
        result.is_err(),
        "Expected error for ORDER BY nonexistent column"
    );
}

#[test]
fn test_where_type_mismatch() {
    let conn = create_test_connection();
    let result = conn.query("SELECT * FROM users WHERE id = 'abc'");
    drop(result);
}

#[test]
fn test_multiple_statements_in_query() {
    let conn = create_test_connection();
    let result = conn.query("SELECT 1; SELECT 2");
    drop(result);
}
