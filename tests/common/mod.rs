//! Shared test utilities and helpers for integration tests.

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use blaze::Connection;

/// Create a test connection with sample `users` and `orders` tables.
pub fn create_test_connection() -> Connection {
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
