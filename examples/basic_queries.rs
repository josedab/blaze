//! Basic usage examples for Blaze Query Engine
//!
//! Run with: `cargo run --example basic_queries`

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use blaze::{Connection, ConnectionConfig};

fn main() {
    // Create an in-memory connection
    let conn = Connection::in_memory().unwrap();

    // Create sample tables
    setup_tables(&conn);

    // Basic SELECT
    println!("=== Basic SELECT ===");
    let results = conn.query("SELECT * FROM users").unwrap();
    print_results(&results);

    // SELECT with projection
    println!("\n=== SELECT with projection ===");
    let results = conn.query("SELECT name, age FROM users").unwrap();
    print_results(&results);

    // SELECT with WHERE
    println!("\n=== SELECT with WHERE ===");
    let results = conn.query("SELECT name, age FROM users WHERE age > 25").unwrap();
    print_results(&results);

    // SELECT with ORDER BY
    println!("\n=== SELECT with ORDER BY ===");
    let results = conn.query("SELECT name, age FROM users ORDER BY age DESC").unwrap();
    print_results(&results);

    // SELECT with LIMIT
    println!("\n=== SELECT with LIMIT ===");
    let results = conn.query("SELECT name FROM users LIMIT 3").unwrap();
    print_results(&results);

    // Aggregate functions
    println!("\n=== Aggregate functions ===");
    let results = conn.query("SELECT COUNT(*) as total, AVG(age) as avg_age FROM users").unwrap();
    print_results(&results);

    // GROUP BY
    println!("\n=== GROUP BY ===");
    let results = conn.query(
        "SELECT department, COUNT(*) as count FROM employees GROUP BY department"
    ).unwrap();
    print_results(&results);

    // JOIN
    println!("\n=== INNER JOIN ===");
    let results = conn.query(
        "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id"
    ).unwrap();
    print_results(&results);

    // Window functions
    println!("\n=== Window functions ===");
    let results = conn.query(
        "SELECT name, age, ROW_NUMBER() OVER (ORDER BY age) as rn FROM users"
    ).unwrap();
    print_results(&results);

    // CTE (Common Table Expression)
    println!("\n=== CTE ===");
    let results = conn.query(
        "WITH senior_users AS (SELECT * FROM users WHERE age >= 30)
         SELECT name, age FROM senior_users"
    ).unwrap();
    print_results(&results);

    // CASE WHEN
    println!("\n=== CASE WHEN ===");
    let results = conn.query(
        "SELECT name, CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END as category FROM users"
    ).unwrap();
    print_results(&results);

    // String functions
    println!("\n=== String functions ===");
    let results = conn.query("SELECT UPPER(name) as upper_name FROM users WHERE name = 'Alice'").unwrap();
    print_results(&results);

    // Literal expressions
    println!("\n=== Literal expressions ===");
    let results = conn.query("SELECT 1 + 2 AS result").unwrap();
    print_results(&results);

    // Custom configuration
    println!("\n=== Custom configuration ===");
    let config = ConnectionConfig::new()
        .with_batch_size(1024)
        .with_memory_limit(100 * 1024 * 1024);
    let custom_conn = Connection::with_config(config).unwrap();
    println!("Created connection with custom batch size and memory limit");
    drop(custom_conn);

    println!("\nAll examples completed successfully!");
}

fn setup_tables(conn: &Connection) {
    // Create users table
    let users_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("age", ArrowDataType::Int64, false),
    ]));

    let users_batch = RecordBatch::try_new(
        users_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"])),
            Arc::new(Int64Array::from(vec![30, 25, 35, 28, 32])),
        ],
    ).unwrap();

    conn.register_batches("users", vec![users_batch]).unwrap();

    // Create orders table
    let orders_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("user_id", ArrowDataType::Int64, false),
        ArrowField::new("amount", ArrowDataType::Float64, false),
    ]));

    let orders_batch = RecordBatch::try_new(
        orders_schema,
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(Float64Array::from(vec![100.0, 150.0, 200.0])),
        ],
    ).unwrap();

    conn.register_batches("orders", vec![orders_batch]).unwrap();

    // Create employees table
    let employees_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("department", ArrowDataType::Utf8, false),
    ]));

    let employees_batch = RecordBatch::try_new(
        employees_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana"])),
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "Engineering", "Sales"])),
        ],
    ).unwrap();

    conn.register_batches("employees", vec![employees_batch]).unwrap();
}

fn print_results(batches: &[RecordBatch]) {
    for batch in batches {
        println!("Schema: {:?}", batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        println!("Rows: {}", batch.num_rows());
        for row in 0..batch.num_rows().min(5) {
            let mut row_values = Vec::new();
            for col in 0..batch.num_columns() {
                let col_arr = batch.column(col);
                let value = format_value(col_arr, row);
                row_values.push(value);
            }
            println!("  Row {}: {:?}", row, row_values);
        }
        if batch.num_rows() > 5 {
            println!("  ... and {} more rows", batch.num_rows() - 5);
        }
    }
}

fn format_value(arr: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::Array;

    if arr.is_null(row) {
        return "NULL".to_string();
    }

    if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        return int_arr.value(row).to_string();
    }
    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
        return str_arr.value(row).to_string();
    }
    if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
        return format!("{:.2}", float_arr.value(row));
    }

    "?".to_string()
}
