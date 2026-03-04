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

#[test]
fn test_variance_population() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT VAR_POP(age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // ages: 30,25,35,28,32 => mean=30, var_pop=11.6
    assert!((val - 11.6).abs() < 0.01, "Expected ~11.6, got {}", val);
}

#[test]
fn test_variance_sample() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT VARIANCE(age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // ages: 30,25,35,28,32 => mean=30, var_samp=14.5
    assert!((val - 14.5).abs() < 0.01, "Expected ~14.5, got {}", val);
}

#[test]
fn test_stddev_population() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT STDDEV_POP(age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // sqrt(11.6) ≈ 3.4059
    assert!((val - 3.4059).abs() < 0.01, "Expected ~3.4059, got {}", val);
}

#[test]
fn test_stddev_sample() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT STDDEV(age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // sqrt(14.5) ≈ 3.8079
    assert!((val - 3.8079).abs() < 0.01, "Expected ~3.8079, got {}", val);
}

#[test]
fn test_bool_and() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT BOOL_AND(active) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap()
        .value(0);
    // active: true, true, false, true, false => false
    assert!(!val);
}

#[test]
fn test_bool_or() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT BOOL_OR(active) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap()
        .value(0);
    // active: true, true, false, true, false => true
    assert!(val);
}

#[test]
fn test_every_alias() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT EVERY(active) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap()
        .value(0);
    assert!(!val);
}

#[test]
fn test_any_value() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT ANY_VALUE(name) FROM users")
        .unwrap();
    // Should return some non-null name
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
    assert!(!results[0].column(0).is_null(0));
}

#[test]
fn test_variance_with_group_by() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT user_id, VARIANCE(amount) FROM orders GROUP BY user_id")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[test]
fn test_var_samp_alias() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT VAR_SAMP(age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 14.5).abs() < 0.01, "Expected ~14.5, got {}", val);
}

#[test]
fn test_stddev_samp_alias() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT STDDEV_SAMP(age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 3.8079).abs() < 0.01, "Expected ~3.8079, got {}", val);
}

#[test]
fn test_median() {
    let conn = create_test_connection();
    let results = conn.query("SELECT MEDIAN(age) FROM users").unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // ages: 25, 28, 30, 32, 35 (sorted) => median = 30.0
    assert!((val - 30.0).abs() < 0.01, "Expected 30.0, got {}", val);
}

#[test]
fn test_percentile_cont() {
    let conn = create_test_connection();
    let results = conn.query("SELECT PERCENTILE_CONT(age) FROM users").unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // Default 0.5 => same as MEDIAN => 30.0
    assert!((val - 30.0).abs() < 0.01, "Expected 30.0, got {}", val);
}

#[test]
fn test_percentile_disc() {
    let conn = create_test_connection();
    let results = conn.query("SELECT PERCENTILE_DISC(age) FROM users").unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // Default 0.5, discrete => nearest rank
    // sorted: 25,28,30,32,35 => ceil(0.5*5)=3 => index 2 => 30.0
    assert!((val - 30.0).abs() < 0.01, "Expected 30.0, got {}", val);
}

#[test]
fn test_median_with_group_by() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT user_id, MEDIAN(amount) FROM orders GROUP BY user_id")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[test]
fn test_percentile_cont_within_group() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // ages sorted: 25,28,30,32,35 => median = 30.0
    assert!((val - 30.0).abs() < 0.01, "Expected 30.0, got {}", val);
}

#[test]
fn test_percentile_disc_within_group() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY age) FROM users")
        .unwrap();
    let val = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // ages sorted: 25,28,30,32,35 => 75th percentile discrete = 32.0
    assert!((val - 32.0).abs() < 0.01, "Expected 32.0, got {}", val);
}

#[test]
fn test_percentile_cont_within_group_with_group_by() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT user_id, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FROM orders GROUP BY user_id")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}
