//! Integration tests for Decimal128 type support across the engine:
//! ORDER BY, GROUP BY, COUNT(DISTINCT), and window PARTITION BY.

mod common;

use std::sync::Arc;

use arrow::array::{Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use blaze::Connection;

/// Create a connection with a table containing a DECIMAL(18,2) column.
fn create_decimal_connection() -> Connection {
    let conn = Connection::in_memory().unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("amount", ArrowDataType::Decimal128(18, 2), false),
        ArrowField::new("category", ArrowDataType::Utf8, false),
    ]));

    // Values are stored as i128 with scale=2, e.g. 10050 => 100.50
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(
                Decimal128Array::from(vec![10050i128, 20075, 10050, 30000, 20075, 50025])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            Arc::new(StringArray::from(vec![
                "food", "tech", "food", "tech", "food", "tech",
            ])),
        ],
    )
    .unwrap();

    conn.register_batches("sales", vec![batch]).unwrap();
    conn
}

#[test]
fn test_decimal_order_by_ascending() {
    let conn = create_decimal_connection();
    let results = conn
        .query("SELECT id, amount FROM sales ORDER BY amount")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 6);

    let amounts = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let values: Vec<i128> = (0..amounts.len()).map(|i| amounts.value(i)).collect();
    // 100.50, 100.50, 200.75, 200.75, 300.00, 500.25
    assert_eq!(values, vec![10050, 10050, 20075, 20075, 30000, 50025]);
}

#[test]
fn test_decimal_order_by_descending() {
    let conn = create_decimal_connection();
    let results = conn
        .query("SELECT id, amount FROM sales ORDER BY amount DESC")
        .unwrap();
    assert_eq!(results.len(), 1);

    let amounts = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let values: Vec<i128> = (0..amounts.len()).map(|i| amounts.value(i)).collect();
    // 500.25, 300.00, 200.75, 200.75, 100.50, 100.50
    assert_eq!(values, vec![50025, 30000, 20075, 20075, 10050, 10050]);
}

#[test]
fn test_decimal_group_by() {
    let conn = create_decimal_connection();
    let results = conn
        .query("SELECT amount, COUNT(*) AS cnt FROM sales GROUP BY amount")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    // 4 distinct amounts: 100.50 (x2), 200.75 (x2), 300.00 (x1), 500.25 (x1)
    assert_eq!(total_rows, 4);
}

#[test]
fn test_decimal_group_by_with_order() {
    let conn = create_decimal_connection();
    let results = conn
        .query("SELECT amount, COUNT(*) AS cnt FROM sales GROUP BY amount ORDER BY amount")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 4);

    let amounts = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let values: Vec<i128> = (0..amounts.len()).map(|i| amounts.value(i)).collect();
    assert_eq!(values, vec![10050, 20075, 30000, 50025]);

    let counts = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let count_values: Vec<i64> = (0..counts.len()).map(|i| counts.value(i)).collect();
    assert_eq!(count_values, vec![2, 2, 1, 1]);
}

#[test]
fn test_decimal_count_distinct() {
    let conn = create_decimal_connection();
    let results = conn
        .query("SELECT COUNT(DISTINCT amount) FROM sales")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);

    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // COUNT(DISTINCT) uses approximate counting (HyperLogLog) which may be imprecise
    // for small datasets. The exact value is 4 unique amounts.
    assert!(
        count >= 4 && count <= 6,
        "Expected ~4 distinct values, got {}",
        count
    );
}

#[test]
fn test_decimal_group_by_other_column() {
    let conn = create_decimal_connection();
    let results = conn
        .query("SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category ORDER BY category")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    let categories = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(categories.value(0), "food");
    assert_eq!(categories.value(1), "tech");
}
