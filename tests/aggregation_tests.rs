//! Aggregation and GROUP BY integration tests.

mod common;

use arrow::array::Int64Array;
use common::create_test_connection;

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
fn test_group_by() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[test]
fn test_group_by_count_empty_table() {
    let conn = blaze::Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE empty (id BIGINT, category VARCHAR)")
        .unwrap();
    let results = conn
        .query("SELECT category, COUNT(*) FROM empty GROUP BY category")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}
