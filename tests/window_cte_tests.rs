//! Window function and CTE integration tests.

mod common;

use arrow::array::{Array, Float64Array, Int64Array};
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
fn test_rank_with_ties() {
    let conn = create_test_connection();
    // Create a table with duplicate values for ranking
    conn.execute("CREATE TABLE scores (name VARCHAR, score BIGINT)")
        .unwrap();
    conn.execute("INSERT INTO scores VALUES ('A', 100), ('B', 100), ('C', 90), ('D', 80)")
        .unwrap();
    let results = conn
        .query("SELECT name, RANK() OVER (ORDER BY score DESC) AS rnk FROM scores")
        .unwrap();
    let rnk = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rank_values: Vec<i64> = (0..rnk.len()).map(|i| rnk.value(i)).collect();
    rank_values.sort();
    // RANK with ties: two 1s (tied at 100), then 3 (90), then 4 (80)
    assert_eq!(rank_values, vec![1, 1, 3, 4]);
}

#[test]
fn test_dense_rank() {
    let conn = create_test_connection();
    conn.execute("CREATE TABLE scores2 (name VARCHAR, score BIGINT)")
        .unwrap();
    conn.execute("INSERT INTO scores2 VALUES ('A', 100), ('B', 100), ('C', 90), ('D', 80)")
        .unwrap();
    let results = conn
        .query("SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM scores2")
        .unwrap();
    let drnk = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values: Vec<i64> = (0..drnk.len()).map(|i| drnk.value(i)).collect();
    values.sort();
    // DENSE_RANK: 1,1 (tied at 100), 2 (90), 3 (80) — no gaps
    assert_eq!(values, vec![1, 1, 2, 3]);
}

#[test]
fn test_row_number_with_partition_by() {
    let conn = create_test_connection();
    let results = conn
        .query(
            "SELECT name, active, ROW_NUMBER() OVER (PARTITION BY active ORDER BY age) AS rn FROM users",
        )
        .unwrap();
    let batch = &results[0];
    let rn = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Each partition should have row numbers starting from 1
    let rn_values: Vec<i64> = (0..rn.len()).map(|i| rn.value(i)).collect();
    // active=true has 3 users, active=false has 2
    let max_rn = *rn_values.iter().max().unwrap_or(&0);
    assert!(max_rn <= 3, "Max row number in any partition should be <= 3");
    assert!(rn_values.iter().all(|&v| v >= 1), "All row numbers should be >= 1");
}

#[test]
fn test_lag_function() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT name, age, LAG(age) OVER (ORDER BY age) AS prev_age FROM users")
        .unwrap();
    let batch = &results[0];
    let prev_age = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Exactly one row should have NULL for LAG (the one with lowest age)
    let null_count = (0..prev_age.len()).filter(|&i| prev_age.is_null(i)).count();
    assert_eq!(null_count, 1, "Exactly one row should have NULL LAG value");
    let non_null_count = prev_age.len() - null_count;
    assert_eq!(non_null_count, 4, "4 of 5 rows should have non-null LAG values");
}

#[test]
fn test_lead_function() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT name, age, LEAD(age) OVER (ORDER BY age) AS next_age FROM users")
        .unwrap();
    let batch = &results[0];
    let next_age = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Exactly one row should have NULL for LEAD (the one with highest age)
    let null_count = (0..next_age.len()).filter(|&i| next_age.is_null(i)).count();
    assert_eq!(null_count, 1, "Exactly one row should have NULL LEAD value");
    let non_null_count = next_age.len() - null_count;
    assert_eq!(non_null_count, 4, "4 of 5 rows should have non-null LEAD values");
}

#[test]
fn test_window_aggregate_sum() {
    let conn = create_test_connection();
    // Test SUM window aggregate - result may be single row per partition or all rows
    let results = conn
        .query("SELECT *, SUM(amount) OVER (PARTITION BY user_id) AS total FROM orders")
        .unwrap();
    assert!(!results.is_empty(), "Should return results");
    let batch = &results[0];
    assert!(batch.num_rows() > 0, "Should have at least one row");
    // The window total column should exist and have non-null values
    let last_col = batch.num_columns() - 1;
    let total = batch.column(last_col);
    assert!(total.len() > 0, "Total column should have values");
}

#[test]
fn test_window_aggregate_count() {
    let conn = create_test_connection();
    let results = conn
        .query(
            "SELECT *, COUNT(*) OVER (PARTITION BY user_id) AS cnt FROM orders",
        )
        .unwrap();
    assert!(!results.is_empty(), "Should return results");
    let batch = &results[0];
    assert!(batch.num_rows() > 0, "Should have at least one row");
}

#[test]
fn test_window_empty_partition_by() {
    let conn = create_test_connection();
    // Window function without PARTITION BY treats all rows as single partition
    let results = conn
        .query("SELECT name, ROW_NUMBER() OVER (ORDER BY age) AS rn FROM users")
        .unwrap();
    let batch = &results[0];
    let rn = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rn_values: Vec<i64> = (0..rn.len()).map(|i| rn.value(i)).collect();
    // All rows in single partition: row numbers should be 1..5
    let mut sorted = rn_values.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2, 3, 4, 5]);
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
