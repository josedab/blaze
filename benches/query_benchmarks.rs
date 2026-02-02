//! Blaze Query Engine Benchmarks
//!
//! Comprehensive benchmark suite for measuring query performance.
//!
//! Run with: cargo bench
//! Run specific benchmark: cargo bench -- <name>
//! Generate HTML report: cargo bench -- --save-baseline main

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use blaze::Connection;

/// Generate a table with N rows for benchmarking.
fn create_large_test_connection(num_rows: usize) -> Connection {
    let conn = Connection::in_memory().expect("Failed to create connection");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]));

    // Generate data
    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let values: Vec<f64> = (0..num_rows).map(|i| (i as f64) * 1.5).collect();
    let categories: Vec<&str> = (0..num_rows)
        .map(|i| match i % 4 {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D",
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(categories)),
        ],
    )
    .expect("Failed to create batch");

    conn.register_batches("bench_table", vec![batch])
        .expect("Failed to register table");

    conn
}

/// Create a connection with multiple tables for join benchmarks.
fn create_join_test_connection(left_rows: usize, right_rows: usize) -> Connection {
    let conn = Connection::in_memory().expect("Failed to create connection");

    // Left table
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("left_value", DataType::Float64, true),
    ]));

    let left_ids: Vec<i64> = (0..left_rows as i64).collect();
    let left_values: Vec<f64> = (0..left_rows).map(|i| i as f64).collect();

    let left_batch = RecordBatch::try_new(
        left_schema,
        vec![
            Arc::new(Int64Array::from(left_ids)),
            Arc::new(Float64Array::from(left_values)),
        ],
    )
    .expect("Failed to create batch");

    conn.register_batches("left_table", vec![left_batch])
        .expect("Failed to register table");

    // Right table
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("right_value", DataType::Float64, true),
    ]));

    let right_ids: Vec<i64> = (0..right_rows as i64).collect();
    let right_values: Vec<f64> = (0..right_rows).map(|i| i as f64 * 2.0).collect();

    let right_batch = RecordBatch::try_new(
        right_schema,
        vec![
            Arc::new(Int64Array::from(right_ids)),
            Arc::new(Float64Array::from(right_values)),
        ],
    )
    .expect("Failed to create batch");

    conn.register_batches("right_table", vec![right_batch])
        .expect("Failed to register table");

    conn
}

// ============================================================================
// Table Scan Benchmarks
// ============================================================================

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");

    for size in [100, 1_000, 10_000].iter() {
        let conn = create_large_test_connection(*size);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| black_box(conn.query("SELECT * FROM bench_table").unwrap()))
        });
    }

    group.finish();
}

// ============================================================================
// Filter Benchmarks
// ============================================================================

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");

    for size in [1_000, 10_000].iter() {
        let conn = create_large_test_connection(*size);

        group.throughput(Throughput::Elements(*size as u64));

        // Simple equality filter
        group.bench_with_input(BenchmarkId::new("equality", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query("SELECT * FROM bench_table WHERE id = 500")
                        .unwrap(),
                )
            })
        });

        // Range filter
        group.bench_with_input(BenchmarkId::new("range", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query("SELECT * FROM bench_table WHERE id > 100 AND id < 500")
                        .unwrap(),
                )
            })
        });

        // String filter
        group.bench_with_input(BenchmarkId::new("string", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query("SELECT * FROM bench_table WHERE category = 'A'")
                        .unwrap(),
                )
            })
        });
    }

    group.finish();
}

// ============================================================================
// Aggregation Benchmarks
// ============================================================================

fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");

    for size in [1_000, 10_000].iter() {
        let conn = create_large_test_connection(*size);

        group.throughput(Throughput::Elements(*size as u64));

        // Simple COUNT
        group.bench_with_input(BenchmarkId::new("count", size), size, |b, _| {
            b.iter(|| black_box(conn.query("SELECT COUNT(*) FROM bench_table").unwrap()))
        });

        // SUM
        group.bench_with_input(BenchmarkId::new("sum", size), size, |b, _| {
            b.iter(|| black_box(conn.query("SELECT SUM(value) FROM bench_table").unwrap()))
        });

        // AVG
        group.bench_with_input(BenchmarkId::new("avg", size), size, |b, _| {
            b.iter(|| black_box(conn.query("SELECT AVG(value) FROM bench_table").unwrap()))
        });

        // Multiple aggregates
        group.bench_with_input(
            BenchmarkId::new("multiple", size),
            size,
            |b, _| {
                b.iter(|| {
                    black_box(
                        conn.query(
                            "SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM bench_table",
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // GROUP BY
        group.bench_with_input(BenchmarkId::new("group_by", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query(
                        "SELECT category, COUNT(*), AVG(value) FROM bench_table GROUP BY category",
                    )
                    .unwrap(),
                )
            })
        });
    }

    group.finish();
}

// ============================================================================
// Sort Benchmarks
// ============================================================================

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");

    for size in [1_000, 10_000].iter() {
        let conn = create_large_test_connection(*size);

        group.throughput(Throughput::Elements(*size as u64));

        // Single column sort
        group.bench_with_input(BenchmarkId::new("single_column", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query("SELECT * FROM bench_table ORDER BY value")
                        .unwrap(),
                )
            })
        });

        // Descending sort
        group.bench_with_input(BenchmarkId::new("descending", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query("SELECT * FROM bench_table ORDER BY value DESC")
                        .unwrap(),
                )
            })
        });

        // Multi-column sort
        group.bench_with_input(BenchmarkId::new("multi_column", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query("SELECT * FROM bench_table ORDER BY category, value DESC")
                        .unwrap(),
                )
            })
        });
    }

    group.finish();
}

// ============================================================================
// Join Benchmarks
// ============================================================================

fn bench_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join");

    for (left_size, right_size) in [(100, 100), (1_000, 100), (1_000, 1_000)].iter() {
        let conn = create_join_test_connection(*left_size, *right_size);

        let total_elements = (*left_size + *right_size) as u64;
        group.throughput(Throughput::Elements(total_elements));

        let label = format!("{}x{}", left_size, right_size);

        // Inner join
        group.bench_with_input(
            BenchmarkId::new("inner", &label),
            &(left_size, right_size),
            |b, _| {
                b.iter(|| {
                    black_box(
                        conn.query(
                            "SELECT l.id, l.left_value, r.right_value
                             FROM left_table l
                             INNER JOIN right_table r ON l.id = r.id",
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // Left join
        group.bench_with_input(
            BenchmarkId::new("left", &label),
            &(left_size, right_size),
            |b, _| {
                b.iter(|| {
                    black_box(
                        conn.query(
                            "SELECT l.id, l.left_value, r.right_value
                             FROM left_table l
                             LEFT JOIN right_table r ON l.id = r.id",
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Window Function Benchmarks
// ============================================================================

fn bench_window_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("window");

    for size in [1_000, 10_000].iter() {
        let conn = create_large_test_connection(*size);

        group.throughput(Throughput::Elements(*size as u64));

        // ROW_NUMBER
        group.bench_with_input(BenchmarkId::new("row_number", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM bench_table")
                        .unwrap(),
                )
            })
        });

        // RANK with partition
        group.bench_with_input(
            BenchmarkId::new("rank_partition", size),
            size,
            |b, _| {
                b.iter(|| {
                    black_box(
                        conn.query(
                            "SELECT id, RANK() OVER (PARTITION BY category ORDER BY value) FROM bench_table",
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // LAG
        group.bench_with_input(BenchmarkId::new("lag", size), size, |b, _| {
            b.iter(|| {
                black_box(
                    conn.query(
                        "SELECT id, value, LAG(value, 1) OVER (ORDER BY id) FROM bench_table",
                    )
                    .unwrap(),
                )
            })
        });

        // Running sum
        group.bench_with_input(
            BenchmarkId::new("running_sum", size),
            size,
            |b, _| {
                b.iter(|| {
                    black_box(
                        conn.query(
                            "SELECT id, SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM bench_table",
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Complex Query Benchmarks (TPC-H inspired)
// ============================================================================

fn bench_complex_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex");

    let size = 5_000;
    let conn = create_large_test_connection(size);

    group.throughput(Throughput::Elements(size as u64));

    // Filter + Aggregate + Sort
    group.bench_function("filter_agg_sort", |b| {
        b.iter(|| {
            black_box(
                conn.query(
                    "SELECT category, COUNT(*) as cnt, AVG(value) as avg_val
                     FROM bench_table
                     WHERE id > 1000
                     GROUP BY category
                     ORDER BY avg_val DESC",
                )
                .unwrap(),
            )
        })
    });

    // Subquery
    group.bench_function("subquery", |b| {
        b.iter(|| {
            black_box(
                conn.query(
                    "SELECT * FROM bench_table
                     WHERE value > (SELECT AVG(value) FROM bench_table)",
                )
                .unwrap(),
            )
        })
    });

    // CTE
    group.bench_function("cte", |b| {
        b.iter(|| {
            black_box(
                conn.query(
                    "WITH category_stats AS (
                         SELECT category, AVG(value) as avg_val
                         FROM bench_table
                         GROUP BY category
                     )
                     SELECT b.*, cs.avg_val
                     FROM bench_table b
                     JOIN category_stats cs ON b.category = cs.category
                     WHERE b.value > cs.avg_val",
                )
                .unwrap(),
            )
        })
    });

    group.finish();
}

// ============================================================================
// Expression Benchmarks
// ============================================================================

fn bench_expressions(c: &mut Criterion) {
    let mut group = c.benchmark_group("expressions");

    let size = 5_000;
    let conn = create_large_test_connection(size);

    // Arithmetic
    group.bench_function("arithmetic", |b| {
        b.iter(|| {
            black_box(
                conn.query("SELECT id, value * 2 + 10 / 5 - 3 FROM bench_table")
                    .unwrap(),
            )
        })
    });

    // String functions
    group.bench_function("string_upper", |b| {
        b.iter(|| {
            black_box(
                conn.query("SELECT id, UPPER(category) FROM bench_table")
                    .unwrap(),
            )
        })
    });

    // CASE expression
    group.bench_function("case_expression", |b| {
        b.iter(|| {
            black_box(
                conn.query(
                    "SELECT id, CASE
                         WHEN value < 100 THEN 'low'
                         WHEN value < 500 THEN 'medium'
                         ELSE 'high'
                     END as level
                     FROM bench_table",
                )
                .unwrap(),
            )
        })
    });

    // COALESCE
    group.bench_function("coalesce", |b| {
        b.iter(|| {
            black_box(
                conn.query("SELECT id, COALESCE(value, 0) FROM bench_table")
                    .unwrap(),
            )
        })
    });

    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(std::time::Duration::from_secs(5));
    targets =
        bench_scan,
        bench_filter,
        bench_aggregate,
        bench_sort,
        bench_join,
        bench_window_functions,
        bench_complex_queries,
        bench_expressions,
);

criterion_main!(benches);
