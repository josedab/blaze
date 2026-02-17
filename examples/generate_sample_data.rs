//! Script to generate sample Parquet file for the data/ directory.
//!
//! Run with: cargo run --example generate_sample_data

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

fn main() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("signup_date", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec![
                "Alice Johnson",
                "Bob Smith",
                "Charlie Brown",
                "Diana Prince",
                "Eve Davis",
                "Frank Miller",
                "Grace Lee",
                "Henry Wilson",
                "Ivy Chen",
                "Jack Taylor",
            ])),
            Arc::new(StringArray::from(vec![
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "diana@example.com",
                "eve@example.com",
                "frank@example.com",
                "grace@example.com",
                "henry@example.com",
                "ivy@example.com",
                "jack@example.com",
            ])),
            Arc::new(StringArray::from(vec![
                "New York",
                "Los Angeles",
                "Chicago",
                "Houston",
                "Phoenix",
                "Philadelphia",
                "San Antonio",
                "San Diego",
                "Dallas",
                "San Jose",
            ])),
            Arc::new(StringArray::from(vec![
                "2023-06-15",
                "2023-07-20",
                "2023-08-10",
                "2023-09-05",
                "2023-10-01",
                "2023-10-15",
                "2023-11-01",
                "2023-11-20",
                "2023-12-01",
                "2024-01-10",
            ])),
        ],
    )
    .expect("Failed to create RecordBatch");

    let file = File::create("data/customers.parquet").expect("Failed to create file");
    let mut writer =
        ArrowWriter::try_new(file, schema, None).expect("Failed to create Parquet writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    println!(
        "Generated data/customers.parquet ({} rows)",
        batch.num_rows()
    );
}
