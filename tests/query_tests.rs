//! Basic query integration tests: SELECT, LIMIT, columns, aliases, literals.

mod common;

use arrow::array::{Int64Array, StringArray};
use blaze::Connection;
use common::create_test_connection;

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
    assert_eq!(total_rows, 5);
}

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
fn test_case_when() {
    let conn = create_test_connection();
    let results = conn.query(
        "SELECT name, CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END AS category FROM users",
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

#[test]
fn test_limit_larger_than_rows() {
    let conn = create_test_connection();
    let results = conn.query("SELECT * FROM users LIMIT 100").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);
}

#[test]
fn test_offset_larger_than_rows() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT * FROM users LIMIT 10 OFFSET 100")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}
