//! Window function and CTE integration tests.

mod common;

use arrow::array::Int64Array;
use common::create_test_connection;

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
