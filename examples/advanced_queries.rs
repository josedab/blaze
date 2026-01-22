//! Advanced query examples for Blaze Query Engine
//!
//! This example demonstrates advanced SQL features including:
//! - Window functions
//! - CTEs (Common Table Expressions)
//! - Subqueries
//! - Complex joins
//! - Date/time functions
//! - String functions
//!
//! Run with: `cargo run --example advanced_queries`

use arrow::array::{Date32Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use blaze::Connection;

fn run_query(conn: &Connection, sql: &str) {
    match conn.query(sql) {
        Ok(results) => print_results(&results),
        Err(e) => println!("  (Not yet supported: {})", e),
    }
}

fn main() {
    let conn = Connection::in_memory().unwrap();
    setup_sample_data(&conn);

    println!("{}", "=".repeat(60));
    println!("Blaze SQL - Advanced Query Examples");
    println!("{}", "=".repeat(60));
    println!();

    // 1. Window Functions
    window_function_examples(&conn);

    // 2. CTEs
    cte_examples(&conn);

    // 3. Subqueries
    subquery_examples(&conn);

    // 4. Complex Joins
    complex_join_examples(&conn);

    // 5. String Functions
    string_function_examples(&conn);

    // 6. Aggregation with HAVING
    aggregation_examples(&conn);

    // 7. CASE expressions
    case_expression_examples(&conn);

    // 8. Set Operations
    set_operation_examples(&conn);

    println!("\nAll advanced examples completed successfully!");
}

fn window_function_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("1. WINDOW FUNCTIONS");
    println!("{}", "=".repeat(60));

    // ROW_NUMBER
    println!("\n--- ROW_NUMBER ---");
    run_query(conn,
        "SELECT name, department, salary,
                ROW_NUMBER() OVER (ORDER BY salary DESC) as overall_rank
         FROM employees");

    // RANK with PARTITION
    println!("\n--- RANK with PARTITION ---");
    run_query(conn,
        "SELECT name, department, salary,
                RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
         FROM employees");

    // DENSE_RANK
    println!("\n--- DENSE_RANK ---");
    run_query(conn,
        "SELECT name, department, salary,
                DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank
         FROM employees");

    // LAG and LEAD
    println!("\n--- LAG and LEAD ---");
    run_query(conn,
        "SELECT name, salary,
                LAG(salary, 1) OVER (ORDER BY salary) as prev_salary,
                LEAD(salary, 1) OVER (ORDER BY salary) as next_salary
         FROM employees");

    // Running sum
    println!("\n--- Running Sum ---");
    run_query(conn,
        "SELECT name, salary,
                SUM(salary) OVER (ORDER BY salary) as running_total
         FROM employees
         ORDER BY salary");

    // NTILE
    println!("\n--- NTILE (Quartiles) ---");
    run_query(conn,
        "SELECT name, salary,
                NTILE(4) OVER (ORDER BY salary) as salary_quartile
         FROM employees");
}

fn cte_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("2. COMMON TABLE EXPRESSIONS (CTEs)");
    println!("{}", "=".repeat(60));

    // Simple CTE
    println!("\n--- Simple CTE ---");
    run_query(conn,
        "WITH high_earners AS (
             SELECT * FROM employees WHERE salary > 80000.0
         )
         SELECT name, department, salary FROM high_earners ORDER BY salary DESC");

    // Multiple CTEs
    println!("\n--- Multiple CTEs ---");
    run_query(conn,
        "WITH
             dept_stats AS (
                 SELECT department, AVG(salary) as avg_salary
                 FROM employees
                 GROUP BY department
             ),
             above_avg AS (
                 SELECT e.*, d.avg_salary
                 FROM employees e
                 JOIN dept_stats d ON e.department = d.department
                 WHERE e.salary > d.avg_salary
             )
         SELECT name, department, salary, avg_salary
         FROM above_avg
         ORDER BY department, salary DESC");

    // CTE with aggregation
    println!("\n--- CTE with Aggregation ---");
    run_query(conn,
        "WITH monthly_sales AS (
             SELECT product, SUM(amount) as total_sales
             FROM sales
             GROUP BY product
         )
         SELECT product, total_sales,
                RANK() OVER (ORDER BY total_sales DESC) as sales_rank
         FROM monthly_sales");
}

fn subquery_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("3. SUBQUERIES");
    println!("{}", "=".repeat(60));

    // Scalar subquery in WHERE
    println!("\n--- Scalar Subquery in WHERE ---");
    run_query(conn,
        "SELECT name, salary
         FROM employees
         WHERE salary > (SELECT AVG(salary) FROM employees)");

    // IN subquery
    println!("\n--- IN Subquery ---");
    run_query(conn,
        "SELECT name, department
         FROM employees
         WHERE department IN (
             SELECT department
             FROM employees
             GROUP BY department
             HAVING COUNT(*) > 1
         )");

    // Correlated subquery
    println!("\n--- Subquery in SELECT ---");
    run_query(conn,
        "SELECT
             name,
             salary,
             (SELECT AVG(salary) FROM employees) as company_avg
         FROM employees
         ORDER BY salary DESC");
}

fn complex_join_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("4. COMPLEX JOINS");
    println!("{}", "=".repeat(60));

    // LEFT JOIN
    println!("\n--- LEFT JOIN ---");
    run_query(conn,
        "SELECT e.name, e.department, COALESCE(SUM(s.amount), 0) as total_sales
         FROM employees e
         LEFT JOIN sales s ON e.name = s.salesperson
         GROUP BY e.name, e.department
         ORDER BY total_sales DESC");

    // FULL OUTER JOIN
    println!("\n--- FULL OUTER JOIN ---");
    run_query(conn,
        "SELECT
             COALESCE(e.name, 'Unknown') as employee,
             COALESCE(s.product, 'No Sales') as product,
             s.amount
         FROM employees e
         FULL OUTER JOIN sales s ON e.name = s.salesperson");

    // Self JOIN
    println!("\n--- Self JOIN (Salary Comparison) ---");
    run_query(conn,
        "SELECT e1.name, e1.salary, e2.name as higher_paid
         FROM employees e1
         JOIN employees e2 ON e1.department = e2.department AND e1.salary < e2.salary
         ORDER BY e1.department, e1.salary");

    // Three-way JOIN
    println!("\n--- Three-way JOIN ---");
    run_query(conn,
        "SELECT e.name, e.department, d.location, s.amount
         FROM employees e
         JOIN departments d ON e.department = d.name
         JOIN sales s ON e.name = s.salesperson
         ORDER BY s.amount DESC");
}

fn string_function_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("5. STRING FUNCTIONS");
    println!("{}", "=".repeat(60));

    println!("\n--- UPPER and LOWER ---");
    run_query(conn,
        "SELECT name, UPPER(name) as upper_name, LOWER(department) as lower_dept
         FROM employees
         LIMIT 3");

    println!("\n--- CONCAT and LENGTH ---");
    run_query(conn,
        "SELECT CONCAT(name, ' - ', department) as full_info, LENGTH(name) as name_length
         FROM employees
         LIMIT 3");

    println!("\n--- SUBSTRING ---");
    run_query(conn,
        "SELECT name, SUBSTRING(name, 1, 3) as short_name
         FROM employees
         LIMIT 3");

    println!("\n--- REPLACE ---");
    run_query(conn,
        "SELECT department, REPLACE(department, 'Engineering', 'Tech') as updated_dept
         FROM employees
         WHERE department = 'Engineering'
         LIMIT 3");

    println!("\n--- TRIM ---");
    run_query(conn,
        "SELECT TRIM('  hello  ') as trimmed,
                LTRIM('  left') as left_trimmed,
                RTRIM('right  ') as right_trimmed");
}

fn aggregation_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("6. AGGREGATION WITH HAVING");
    println!("{}", "=".repeat(60));

    println!("\n--- GROUP BY with HAVING ---");
    run_query(conn,
        "SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
         FROM employees
         GROUP BY department
         HAVING COUNT(*) > 1
         ORDER BY avg_salary DESC");

    println!("\n--- Multiple Aggregates ---");
    run_query(conn,
        "SELECT
             COUNT(*) as total_employees,
             SUM(salary) as total_payroll,
             AVG(salary) as avg_salary,
             MIN(salary) as min_salary,
             MAX(salary) as max_salary
         FROM employees");

    println!("\n--- COUNT DISTINCT ---");
    run_query(conn,
        "SELECT COUNT(DISTINCT department) as num_departments,
                COUNT(DISTINCT product) as num_products
         FROM employees, sales");
}

fn case_expression_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("7. CASE EXPRESSIONS");
    println!("{}", "=".repeat(60));

    println!("\n--- Simple CASE ---");
    run_query(conn,
        "SELECT name, salary,
                CASE
                    WHEN salary >= 90000.0 THEN 'Senior'
                    WHEN salary >= 70000.0 THEN 'Mid-Level'
                    ELSE 'Junior'
                END as level
         FROM employees
         ORDER BY salary DESC");

    println!("\n--- CASE with Aggregation ---");
    run_query(conn,
        "SELECT department,
                SUM(CASE WHEN salary >= 80000.0 THEN 1 ELSE 0 END) as high_earners,
                SUM(CASE WHEN salary < 80000.0 THEN 1 ELSE 0 END) as regular_earners
         FROM employees
         GROUP BY department");

    println!("\n--- Nested CASE ---");
    run_query(conn,
        "SELECT name, department, salary,
                CASE department
                    WHEN 'Engineering' THEN
                        CASE WHEN salary >= 90000.0 THEN 'Senior Engineer' ELSE 'Engineer' END
                    WHEN 'Sales' THEN
                        CASE WHEN salary >= 75000.0 THEN 'Senior Sales' ELSE 'Sales Rep' END
                    ELSE 'Other'
                END as title
         FROM employees
         ORDER BY department, salary DESC");
}

fn set_operation_examples(conn: &Connection) {
    println!("\n{}", "=".repeat(60));
    println!("8. SET OPERATIONS");
    println!("{}", "=".repeat(60));

    println!("\n--- UNION ---");
    run_query(conn,
        "SELECT name, 'Employee' as type FROM employees WHERE department = 'Engineering'
         UNION
         SELECT salesperson as name, 'Salesperson' as type FROM sales WHERE amount > 1000.0");

    println!("\n--- UNION ALL ---");
    run_query(conn,
        "SELECT name FROM employees WHERE salary > 80000.0
         UNION ALL
         SELECT name FROM employees WHERE department = 'Engineering'");

    println!("\n--- INTERSECT ---");
    run_query(conn,
        "SELECT name FROM employees WHERE salary > 80000.0
         INTERSECT
         SELECT name FROM employees WHERE department = 'Engineering'");

    println!("\n--- EXCEPT ---");
    run_query(conn,
        "SELECT name FROM employees WHERE department = 'Engineering'
         EXCEPT
         SELECT name FROM employees WHERE salary > 90000.0");
}

fn setup_sample_data(conn: &Connection) {
    // Employees table
    let employees_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("department", ArrowDataType::Utf8, false),
        ArrowField::new("salary", ArrowDataType::Float64, false),
    ]));

    let employees_batch = RecordBatch::try_new(
        employees_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
            ])),
            Arc::new(StringArray::from(vec![
                "Engineering",
                "Engineering",
                "Sales",
                "Marketing",
                "Engineering",
                "Sales",
                "Marketing",
                "Engineering",
            ])),
            Arc::new(Float64Array::from(vec![
                95000.0, 87000.0, 72000.0, 68000.0, 102000.0, 78000.0, 71000.0, 91000.0,
            ])),
        ],
    )
    .unwrap();

    conn.register_batches("employees", vec![employees_batch])
        .unwrap();

    // Departments table
    let departments_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("budget", ArrowDataType::Float64, false),
        ArrowField::new("location", ArrowDataType::Utf8, false),
    ]));

    let departments_batch = RecordBatch::try_new(
        departments_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "Marketing"])),
            Arc::new(Float64Array::from(vec![500000.0, 300000.0, 200000.0])),
            Arc::new(StringArray::from(vec![
                "Building A",
                "Building B",
                "Building B",
            ])),
        ],
    )
    .unwrap();

    conn.register_batches("departments", vec![departments_batch])
        .unwrap();

    // Sales table
    let sales_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("salesperson", ArrowDataType::Utf8, false),
        ArrowField::new("product", ArrowDataType::Utf8, false),
        ArrowField::new("amount", ArrowDataType::Float64, false),
        ArrowField::new("order_date", ArrowDataType::Date32, true),
    ]));

    let sales_batch = RecordBatch::try_new(
        sales_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7])),
            Arc::new(StringArray::from(vec![
                "Charlie", "Charlie", "Frank", "Charlie", "Frank", "Alice", "Bob",
            ])),
            Arc::new(StringArray::from(vec![
                "Widget", "Gadget", "Widget", "Gizmo", "Gadget", "Widget", "Gizmo",
            ])),
            Arc::new(Float64Array::from(vec![
                1500.0, 2200.0, 800.0, 3100.0, 1900.0, 950.0, 1200.0,
            ])),
            Arc::new(Date32Array::from(vec![
                Some(19724),
                Some(19725),
                Some(19726),
                Some(19727),
                Some(19728),
                Some(19729),
                Some(19730),
            ])),
        ],
    )
    .unwrap();

    conn.register_batches("sales", vec![sales_batch]).unwrap();
}

fn print_results(batches: &[RecordBatch]) {
    for batch in batches {
        let schema = batch.schema();
        let columns: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        println!("Columns: {:?}", columns);
        println!("Rows: {}", batch.num_rows());

        for row in 0..batch.num_rows().min(10) {
            let mut row_values = Vec::new();
            for col in 0..batch.num_columns() {
                let col_arr = batch.column(col);
                let value = format_value(col_arr, row);
                row_values.push(value);
            }
            println!("  {:?}", row_values);
        }

        if batch.num_rows() > 10 {
            println!("  ... and {} more rows", batch.num_rows() - 10);
        }
    }
}

fn format_value(arr: &dyn arrow::array::Array, row: usize) -> String {
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
    if let Some(date_arr) = arr.as_any().downcast_ref::<Date32Array>() {
        return format!("Date({})", date_arr.value(row));
    }

    "?".to_string()
}
