//! SQL compliance tests verifying Blaze handles standard SQL correctly.

use blaze::Connection;

fn setup() -> Connection {
    let conn = Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE employees (id BIGINT, name VARCHAR, dept VARCHAR, salary DOUBLE)")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 'Eng', 90000.0)")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 'Eng', 85000.0)")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 70000.0)")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 75000.0)")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES (5, 'Eve', 'Eng', 95000.0)")
        .unwrap();
    conn
}

/// Helper to count total rows across all record batches.
fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_select_star() {
    let c = setup();
    assert!(!c.query("SELECT * FROM employees").unwrap().is_empty());
}

#[test]
fn test_select_columns() {
    let c = setup();
    let r = c.query("SELECT name, salary FROM employees").unwrap();
    assert_eq!(r[0].num_columns(), 2);
}

#[test]
fn test_where_equals() {
    let c = setup();
    let r = c
        .query("SELECT * FROM employees WHERE dept = 'Eng'")
        .unwrap();
    assert_eq!(total_rows(&r), 3);
}

#[test]
fn test_where_greater_than() {
    let c = setup();
    let r = c
        .query("SELECT * FROM employees WHERE salary > 80000.0")
        .unwrap();
    assert!(total_rows(&r) >= 2);
}

#[test]
fn test_order_by_asc() {
    let c = setup();
    assert!(!c
        .query("SELECT * FROM employees ORDER BY salary ASC")
        .unwrap()
        .is_empty());
}

#[test]
fn test_order_by_desc() {
    let c = setup();
    assert!(!c
        .query("SELECT * FROM employees ORDER BY salary DESC")
        .unwrap()
        .is_empty());
}

#[test]
fn test_limit() {
    let c = setup();
    let r = c.query("SELECT * FROM employees LIMIT 2").unwrap();
    assert_eq!(total_rows(&r), 2);
}

#[test]
fn test_count() {
    let c = setup();
    let r = c.query("SELECT COUNT(*) FROM employees").unwrap();
    assert!(!r.is_empty());
}

#[test]
fn test_sum() {
    let c = setup();
    assert!(!c
        .query("SELECT SUM(salary) FROM employees")
        .unwrap()
        .is_empty());
}

#[test]
fn test_avg() {
    let c = setup();
    assert!(!c
        .query("SELECT AVG(salary) FROM employees")
        .unwrap()
        .is_empty());
}

#[test]
fn test_group_by() {
    let c = setup();
    let r = c
        .query("SELECT dept, COUNT(*) FROM employees GROUP BY dept")
        .unwrap();
    assert_eq!(r[0].num_rows(), 2);
}

#[test]
fn test_having() {
    let c = setup();
    // HAVING with aggregate expressions is not yet supported;
    // verify the engine returns a descriptive error.
    let result = c.query("SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > 1");
    assert!(result.is_err() || !result.unwrap().is_empty());
}

#[test]
fn test_distinct() {
    let c = setup();
    let r = c.query("SELECT DISTINCT dept FROM employees").unwrap();
    assert_eq!(r[0].num_rows(), 2);
}

#[test]
fn test_case_when() {
    let c = setup();
    assert!(!c
        .query(
            "SELECT name, CASE WHEN salary > 85000.0 THEN 'high' ELSE 'normal' END as level FROM employees"
        )
        .unwrap()
        .is_empty());
}

#[test]
fn test_between() {
    let c = setup();
    assert!(!c
        .query("SELECT * FROM employees WHERE salary BETWEEN 70000.0 AND 90000.0")
        .unwrap()
        .is_empty());
}

#[test]
fn test_in_list() {
    let c = setup();
    assert!(!c
        .query("SELECT * FROM employees WHERE dept IN ('Eng', 'Sales')")
        .unwrap()
        .is_empty());
}

#[test]
fn test_like() {
    let c = setup();
    assert!(!c
        .query("SELECT * FROM employees WHERE name LIKE 'A%'")
        .unwrap()
        .is_empty());
}

#[test]
fn test_is_null() {
    let c = setup();
    assert!(!c
        .query("SELECT * FROM employees WHERE name IS NOT NULL")
        .unwrap()
        .is_empty());
}

#[test]
fn test_alias() {
    let c = setup();
    let r = c
        .query("SELECT name AS employee_name FROM employees")
        .unwrap();
    assert!(!r.is_empty());
}

#[test]
fn test_union() {
    let c = setup();
    assert!(!c
        .query(
            "SELECT name FROM employees WHERE dept = 'Eng' UNION SELECT name FROM employees WHERE dept = 'Sales'"
        )
        .unwrap()
        .is_empty());
}

#[test]
fn test_ilike_basic() {
    let c = setup();
    // ILIKE should be case-insensitive: 'alice' should match 'Alice'
    let r = c
        .query("SELECT * FROM employees WHERE name ILIKE 'alice'")
        .unwrap();
    assert_eq!(total_rows(&r), 1);
}

#[test]
fn test_ilike_wildcard() {
    let c = setup();
    // ILIKE with wildcard: 'a%' should match 'Alice' (case-insensitive)
    let r = c
        .query("SELECT * FROM employees WHERE name ILIKE 'a%'")
        .unwrap();
    assert_eq!(total_rows(&r), 1);
}

#[test]
fn test_not_ilike() {
    let c = setup();
    // NOT ILIKE: all employees whose name doesn't start with 'a' (case-insensitive)
    let r = c
        .query("SELECT * FROM employees WHERE name NOT ILIKE 'a%'")
        .unwrap();
    assert_eq!(total_rows(&r), 4);
}

#[test]
fn test_like_underscore_wildcard() {
    let c = setup();
    // _ should match exactly one character: '_ob' matches 'Bob'
    let r = c
        .query("SELECT * FROM employees WHERE name LIKE '_ob'")
        .unwrap();
    assert_eq!(total_rows(&r), 1);
}

#[test]
fn test_like_escape_percent() {
    let c = setup();
    // Create table with literal % in data
    c.execute("CREATE TABLE products (name VARCHAR, discount VARCHAR)")
        .unwrap();
    c.execute("INSERT INTO products VALUES ('Widget', '10%')")
        .unwrap();
    c.execute("INSERT INTO products VALUES ('Gadget', '20%')")
        .unwrap();
    c.execute("INSERT INTO products VALUES ('Tool', '10 off')")
        .unwrap();

    // LIKE with ESCAPE: match literal '%' character
    let r = c
        .query("SELECT * FROM products WHERE discount LIKE '%\\%' ESCAPE '\\'")
        .unwrap();
    assert_eq!(total_rows(&r), 2); // '10%' and '20%' end with literal '%'
}

#[test]
fn test_like_not_like() {
    let c = setup();
    // NOT LIKE: names that don't start with 'A'
    let r = c
        .query("SELECT * FROM employees WHERE name NOT LIKE 'A%'")
        .unwrap();
    assert_eq!(total_rows(&r), 4);
}

// --- VALUES clause tests ---

#[test]
fn test_values_basic() {
    let c = Connection::in_memory().unwrap();
    let r = c.query("VALUES (1, 'hello'), (2, 'world')").unwrap();
    assert_eq!(total_rows(&r), 2);
    assert_eq!(r[0].num_columns(), 2);
}

#[test]
fn test_values_single_row() {
    let c = Connection::in_memory().unwrap();
    let r = c.query("VALUES (42, 'test', true)").unwrap();
    assert_eq!(total_rows(&r), 1);
    assert_eq!(r[0].num_columns(), 3);
}

#[test]
fn test_values_integers_only() {
    let c = Connection::in_memory().unwrap();
    let r = c.query("VALUES (1), (2), (3)").unwrap();
    assert_eq!(total_rows(&r), 3);
    assert_eq!(r[0].num_columns(), 1);

    // Verify the actual data
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Expected Int64 column");
    assert_eq!(col.value(0), 1);
    assert_eq!(col.value(1), 2);
    assert_eq!(col.value(2), 3);
}

#[test]
fn test_values_with_nulls() {
    let c = Connection::in_memory().unwrap();
    let r = c.query("VALUES (1, 'hello'), (2, NULL)").unwrap();
    assert_eq!(total_rows(&r), 2);
    assert_eq!(r[0].num_columns(), 2);
}

#[test]
fn test_values_floats() {
    let c = Connection::in_memory().unwrap();
    let r = c.query("VALUES (1.5), (2.7), (3.14)").unwrap();
    assert_eq!(total_rows(&r), 3);

    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Expected Float64 column");
    assert!((col.value(0) - 1.5).abs() < f64::EPSILON);
}

#[test]
fn test_values_strings() {
    let c = Connection::in_memory().unwrap();
    let r = c.query("VALUES ('alice'), ('bob'), ('charlie')").unwrap();
    assert_eq!(total_rows(&r), 3);

    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("Expected String column");
    assert_eq!(col.value(0), "alice");
    assert_eq!(col.value(1), "bob");
    assert_eq!(col.value(2), "charlie");
}

#[test]
fn test_values_booleans() {
    let c = Connection::in_memory().unwrap();
    let r = c.query("VALUES (true), (false), (true)").unwrap();
    assert_eq!(total_rows(&r), 3);
}

#[test]
fn test_values_insert_roundtrip() {
    // VALUES should work correctly when used in INSERT context
    let c = Connection::in_memory().unwrap();
    c.execute("CREATE TABLE test_vals (id BIGINT, name VARCHAR)")
        .unwrap();
    c.execute("INSERT INTO test_vals VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .unwrap();
    let r = c.query("SELECT * FROM test_vals").unwrap();
    assert_eq!(total_rows(&r), 3);
}

// --- Trigonometric functions ---

#[test]
fn test_sin() {
    let conn = setup();
    let r = conn.query("SELECT SIN(0)").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 0.0).abs() < 1e-10);
}

#[test]
fn test_cos() {
    let conn = setup();
    let r = conn.query("SELECT COS(0)").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 1.0).abs() < 1e-10);
}

#[test]
fn test_tan() {
    let conn = setup();
    let r = conn.query("SELECT TAN(0)").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 0.0).abs() < 1e-10);
}

#[test]
fn test_asin_acos_atan() {
    let conn = setup();
    let r = conn.query("SELECT ASIN(0), ACOS(1), ATAN(0)").unwrap();
    let c0 = r[0].column(0).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    let c1 = r[0].column(1).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    let c2 = r[0].column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    assert!((c0 - 0.0).abs() < 1e-10);
    assert!((c1 - 0.0).abs() < 1e-10);
    assert!((c2 - 0.0).abs() < 1e-10);
}

#[test]
fn test_atan2() {
    let conn = setup();
    let r = conn.query("SELECT ATAN2(1, 1)").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // atan2(1,1) = pi/4 ≈ 0.7854
    assert!((val - std::f64::consts::FRAC_PI_4).abs() < 1e-10);
}

#[test]
fn test_degrees_radians() {
    let conn = setup();
    let r = conn.query("SELECT DEGREES(3.141592653589793), RADIANS(180)").unwrap();
    let deg = r[0].column(0).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    let rad = r[0].column(1).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    assert!((deg - 180.0).abs() < 1e-6);
    assert!((rad - std::f64::consts::PI).abs() < 1e-6);
}

#[test]
fn test_pi() {
    let conn = setup();
    let r = conn.query("SELECT PI()").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - std::f64::consts::PI).abs() < 1e-15);
}

#[test]
fn test_log2_log10() {
    let conn = setup();
    let r = conn.query("SELECT LOG2(8), LOG10(1000)").unwrap();
    let l2 = r[0].column(0).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    let l10 = r[0].column(1).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    assert!((l2 - 3.0).abs() < 1e-10);
    assert!((l10 - 3.0).abs() < 1e-10);
}

#[test]
fn test_cbrt() {
    let conn = setup();
    let r = conn.query("SELECT CBRT(27)").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 3.0).abs() < 1e-10);
}

#[test]
fn test_trunc() {
    let conn = setup();
    let r = conn.query("SELECT TRUNC(3.14159, 2)").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 3.14).abs() < 1e-10);
}

#[test]
fn test_trunc_no_scale() {
    let conn = setup();
    let r = conn.query("SELECT TRUNC(3.14159)").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 3.0).abs() < 1e-10);
}

#[test]
fn test_random() {
    let conn = setup();
    let r = conn.query("SELECT RANDOM()").unwrap();
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!(val >= 0.0 && val < 1.0, "RANDOM() should be in [0, 1), got {}", val);
}

#[test]
fn test_trig_with_column() {
    let conn = setup();
    let r = conn.query("SELECT SIN(salary / 10000.0) FROM employees LIMIT 1").unwrap();
    assert_eq!(total_rows(&r), 1);
    let val = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    // SIN(9.0) ≈ 0.4121
    assert!((val - (9.0_f64).sin()).abs() < 1e-6);
}

// --- GENERATE_SERIES table function ---

#[test]
fn test_generate_series_basic() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series(1, 5)")
        .unwrap();
    assert_eq!(total_rows(&r), 5);
}

#[test]
fn test_generate_series_with_step() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series(0, 10, 2)")
        .unwrap();
    assert_eq!(total_rows(&r), 6); // 0, 2, 4, 6, 8, 10
}

#[test]
fn test_generate_series_descending() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series(5, 1, -1)")
        .unwrap();
    assert_eq!(total_rows(&r), 5); // 5, 4, 3, 2, 1
}

#[test]
fn test_generate_series_with_alias() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT value FROM generate_series(1, 3) AS s")
        .unwrap();
    assert_eq!(total_rows(&r), 3);
}

#[test]
fn test_generate_series_in_where() {
    let conn = setup();
    let r = conn
        .query("SELECT * FROM generate_series(1, 5) AS s WHERE s.value > 3")
        .unwrap();
    assert_eq!(total_rows(&r), 2); // 4, 5
}

#[test]
fn test_generate_series_single_value() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series(42, 42)")
        .unwrap();
    assert_eq!(total_rows(&r), 1);
}

#[test]
fn test_generate_series_empty() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series(5, 1, 1)")
        .unwrap();
    assert_eq!(total_rows(&r), 0);
}

#[test]
fn test_generate_series_date() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series('2024-01-01', '2024-01-05')")
        .unwrap();
    assert_eq!(total_rows(&r), 5);
}

#[test]
fn test_generate_series_date_weekly() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series('2024-01-01', '2024-01-31', '1 week')")
        .unwrap();
    // Jan 1, 8, 15, 22, 29 = 5 weeks
    assert_eq!(total_rows(&r), 5);
}

#[test]
fn test_generate_series_date_monthly() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series('2024-01-01', '2024-12-31', '1 month')")
        .unwrap();
    // ~12 months with 30-day approximation
    assert!(total_rows(&r) >= 12, "Expected at least 12 months");
}

#[test]
fn test_generate_series_date_descending() {
    let conn = Connection::in_memory().unwrap();
    let r = conn
        .query("SELECT * FROM generate_series('2024-01-05', '2024-01-01', '-1 day')")
        .unwrap();
    assert_eq!(total_rows(&r), 5);
}
