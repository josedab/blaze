//! TPC-H Benchmark Suite for Blaze Query Engine
//!
//! Implements a subset of TPC-H queries with data generation.
//! The data generation produces deterministic, TPC-H-like tables
//! at configurable scale factors.
//!
//! Run with: cargo bench --bench tpch_benchmarks
//! Run specific query: cargo bench --bench tpch_benchmarks -- Q1

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use blaze::Connection;

// ============================================================================
// Deterministic pseudo-random number generator (xorshift64)
// ============================================================================

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Random u64 in [0, max)
    fn next_range(&mut self, max: u64) -> u64 {
        self.next_u64() % max
    }

    /// Random f64 in [low, high)
    fn next_f64_range(&mut self, low: f64, high: f64) -> f64 {
        let frac = (self.next_u64() % 1_000_000) as f64 / 1_000_000.0;
        low + frac * (high - low)
    }
}

// ============================================================================
// Data Generation
// ============================================================================

/// Generate TPC-H lineitem-like table.
///
/// Columns: l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
///           l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
///           l_shipdate
fn generate_lineitem(conn: &Connection, sf: usize) {
    let num_rows = sf * 6000;
    let _num_orders = sf * 1500;
    let num_parts = 200 * sf;
    let num_supps = 10 * sf;

    let mut rng = Rng::new(42);

    let mut l_orderkey = Vec::with_capacity(num_rows);
    let mut l_partkey = Vec::with_capacity(num_rows);
    let mut l_suppkey = Vec::with_capacity(num_rows);
    let mut l_linenumber = Vec::with_capacity(num_rows);
    let mut l_quantity = Vec::with_capacity(num_rows);
    let mut l_extendedprice = Vec::with_capacity(num_rows);
    let mut l_discount = Vec::with_capacity(num_rows);
    let mut l_tax = Vec::with_capacity(num_rows);
    let mut l_returnflag = Vec::with_capacity(num_rows);
    let mut l_linestatus = Vec::with_capacity(num_rows);
    let mut l_shipdate = Vec::with_capacity(num_rows);

    let return_flags = ["R", "A", "N"];
    let line_statuses = ["O", "F"];

    // Base year range for ship dates
    let years = [1993, 1994, 1995, 1996, 1997];
    let months = [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",
    ];

    let mut row = 0;
    let mut order_id: i64 = 1;
    while row < num_rows {
        // Each order has 1-7 line items
        let items_in_order = (rng.next_range(7) + 1) as usize;
        for line_num in 0..items_in_order {
            if row >= num_rows {
                break;
            }
            let qty = rng.next_f64_range(1.0, 50.0);
            let price_per_unit = rng.next_f64_range(1.0, 1000.0);
            let ext_price = (qty * price_per_unit * 100.0).round() / 100.0;
            let disc = (rng.next_range(11) as f64) / 100.0; // 0.00..0.10
            let tax = (rng.next_range(9) as f64) / 100.0; // 0.00..0.08

            l_orderkey.push(order_id);
            l_partkey.push((rng.next_range(num_parts as u64) + 1) as i64);
            l_suppkey.push((rng.next_range(num_supps as u64) + 1) as i64);
            l_linenumber.push((line_num + 1) as i64);
            l_quantity.push(qty);
            l_extendedprice.push(ext_price);
            l_discount.push(disc);
            l_tax.push(tax);
            l_returnflag.push(return_flags[rng.next_range(3) as usize]);
            l_linestatus.push(line_statuses[rng.next_range(2) as usize]);

            let year = years[rng.next_range(years.len() as u64) as usize];
            let month = months[rng.next_range(12) as usize];
            let day = rng.next_range(28) + 1;
            l_shipdate.push(format!("{}-{}-{:02}", year, month, day));

            row += 1;
        }
        order_id += 1;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(l_orderkey)),
            Arc::new(Int64Array::from(l_partkey)),
            Arc::new(Int64Array::from(l_suppkey)),
            Arc::new(Int64Array::from(l_linenumber)),
            Arc::new(Float64Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Float64Array::from(l_tax)),
            Arc::new(StringArray::from(l_returnflag)),
            Arc::new(StringArray::from(l_linestatus)),
            Arc::new(StringArray::from(l_shipdate)),
        ],
    )
    .expect("Failed to create lineitem batch");

    conn.register_batches("lineitem", vec![batch])
        .expect("Failed to register lineitem table");
}

/// Generate TPC-H orders-like table.
///
/// Columns: o_orderkey, o_custkey, o_orderstatus, o_totalprice
fn generate_orders(conn: &Connection, sf: usize) {
    let num_orders = sf * 1500;
    let num_customers = sf * 150;

    let mut rng = Rng::new(123);

    let statuses = ["O", "F", "P"];

    let mut o_orderkey = Vec::with_capacity(num_orders);
    let mut o_custkey = Vec::with_capacity(num_orders);
    let mut o_orderstatus = Vec::with_capacity(num_orders);
    let mut o_totalprice = Vec::with_capacity(num_orders);

    for i in 1..=num_orders {
        o_orderkey.push(i as i64);
        o_custkey.push((rng.next_range(num_customers as u64) + 1) as i64);
        o_orderstatus.push(statuses[rng.next_range(3) as usize]);
        o_totalprice.push(rng.next_f64_range(1000.0, 500_000.0));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(o_orderkey)),
            Arc::new(Int64Array::from(o_custkey)),
            Arc::new(StringArray::from(o_orderstatus)),
            Arc::new(Float64Array::from(o_totalprice)),
        ],
    )
    .expect("Failed to create orders batch");

    conn.register_batches("orders", vec![batch])
        .expect("Failed to register orders table");
}

/// Generate TPC-H customer-like table.
///
/// Columns: c_custkey, c_name, c_nationkey
fn generate_customer(conn: &Connection, sf: usize) {
    let num_customers = sf * 150;

    let mut rng = Rng::new(456);

    let mut c_custkey = Vec::with_capacity(num_customers);
    let mut c_name = Vec::with_capacity(num_customers);
    let mut c_nationkey = Vec::with_capacity(num_customers);

    for i in 1..=num_customers {
        c_custkey.push(i as i64);
        c_name.push(format!("Customer#{:04}", i));
        c_nationkey.push(rng.next_range(25) as i64);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(c_custkey)),
            Arc::new(StringArray::from(c_name)),
            Arc::new(Int64Array::from(c_nationkey)),
        ],
    )
    .expect("Failed to create customer batch");

    conn.register_batches("customer", vec![batch])
        .expect("Failed to register customer table");
}

/// Generate TPC-H nation table (25 rows, always the same).
///
/// Columns: n_nationkey, n_name, n_regionkey
fn generate_nation(conn: &Connection) {
    let nations = [
        (0, "ALGERIA", 0),
        (1, "ARGENTINA", 1),
        (2, "BRAZIL", 1),
        (3, "CANADA", 1),
        (4, "EGYPT", 4),
        (5, "ETHIOPIA", 0),
        (6, "FRANCE", 3),
        (7, "GERMANY", 3),
        (8, "INDIA", 2),
        (9, "INDONESIA", 2),
        (10, "IRAN", 4),
        (11, "IRAQ", 4),
        (12, "JAPAN", 2),
        (13, "JORDAN", 4),
        (14, "KENYA", 0),
        (15, "MOROCCO", 0),
        (16, "MOZAMBIQUE", 0),
        (17, "PERU", 1),
        (18, "CHINA", 2),
        (19, "ROMANIA", 3),
        (20, "SAUDI ARABIA", 4),
        (21, "VIETNAM", 2),
        (22, "RUSSIA", 3),
        (23, "UNITED KINGDOM", 3),
        (24, "UNITED STATES", 1),
    ];

    let n_nationkey: Vec<i64> = nations.iter().map(|(k, _, _)| *k as i64).collect();
    let n_name: Vec<&str> = nations.iter().map(|(_, n, _)| *n).collect();
    let n_regionkey: Vec<i64> = nations.iter().map(|(_, _, r)| *r as i64).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(n_nationkey)),
            Arc::new(StringArray::from(n_name)),
            Arc::new(Int64Array::from(n_regionkey)),
        ],
    )
    .expect("Failed to create nation batch");

    conn.register_batches("nation", vec![batch])
        .expect("Failed to register nation table");
}

/// Generate TPC-H region table (5 rows, always the same).
///
/// Columns: r_regionkey, r_name
fn generate_region(conn: &Connection) {
    let regions = [
        (0, "AFRICA"),
        (1, "AMERICA"),
        (2, "ASIA"),
        (3, "EUROPE"),
        (4, "MIDDLE EAST"),
    ];

    let r_regionkey: Vec<i64> = regions.iter().map(|(k, _)| *k as i64).collect();
    let r_name: Vec<&str> = regions.iter().map(|(_, n)| *n).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(r_regionkey)),
            Arc::new(StringArray::from(r_name)),
        ],
    )
    .expect("Failed to create region batch");

    conn.register_batches("region", vec![batch])
        .expect("Failed to register region table");
}

/// Generate TPC-H supplier-like table.
///
/// Columns: s_suppkey, s_name, s_nationkey
fn generate_supplier(conn: &Connection, sf: usize) {
    let num_suppliers = sf * 10;

    let mut rng = Rng::new(789);

    let mut s_suppkey = Vec::with_capacity(num_suppliers);
    let mut s_name = Vec::with_capacity(num_suppliers);
    let mut s_nationkey = Vec::with_capacity(num_suppliers);

    for i in 1..=num_suppliers {
        s_suppkey.push(i as i64);
        s_name.push(format!("Supplier#{:04}", i));
        s_nationkey.push(rng.next_range(25) as i64);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(s_suppkey)),
            Arc::new(StringArray::from(s_name)),
            Arc::new(Int64Array::from(s_nationkey)),
        ],
    )
    .expect("Failed to create supplier batch");

    conn.register_batches("supplier", vec![batch])
        .expect("Failed to register supplier table");
}

/// Generate TPC-H part-like table.
///
/// Columns: p_partkey, p_name, p_type
fn generate_part(conn: &Connection, sf: usize) {
    let num_parts = sf * 200;

    let mut rng = Rng::new(321);

    let types = [
        "ECONOMY ANODIZED STEEL",
        "ECONOMY BURNISHED BRASS",
        "ECONOMY PLATED COPPER",
        "STANDARD POLISHED TIN",
        "PROMO BRUSHED NICKEL",
        "LARGE ANODIZED STEEL",
        "MEDIUM BURNISHED BRASS",
        "SMALL PLATED COPPER",
    ];

    let mut p_partkey = Vec::with_capacity(num_parts);
    let mut p_name = Vec::with_capacity(num_parts);
    let mut p_type = Vec::with_capacity(num_parts);

    for i in 1..=num_parts {
        p_partkey.push(i as i64);
        p_name.push(format!("Part#{:04}", i));
        p_type.push(types[rng.next_range(types.len() as u64) as usize]);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(p_partkey)),
            Arc::new(StringArray::from(p_name)),
            Arc::new(StringArray::from(
                p_type
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("Failed to create part batch");

    conn.register_batches("part", vec![batch])
        .expect("Failed to register part table");
}

/// Generate TPC-H partsupp-like table.
///
/// Columns: ps_partkey, ps_suppkey, ps_availqty, ps_supplycost
fn generate_partsupp(conn: &Connection, sf: usize) {
    let num_parts = sf * 200;
    let num_supps = sf * 10;
    // Each part has 4 suppliers in TPC-H spec
    let num_rows = num_parts * 4;

    let mut rng = Rng::new(654);

    let mut ps_partkey = Vec::with_capacity(num_rows);
    let mut ps_suppkey = Vec::with_capacity(num_rows);
    let mut ps_availqty = Vec::with_capacity(num_rows);
    let mut ps_supplycost = Vec::with_capacity(num_rows);

    for part in 1..=num_parts {
        for _ in 0..4 {
            ps_partkey.push(part as i64);
            ps_suppkey.push((rng.next_range(num_supps as u64) + 1) as i64);
            ps_availqty.push((rng.next_range(9999) + 1) as i64);
            ps_supplycost.push(rng.next_f64_range(1.0, 1000.0));
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int64, false),
        Field::new("ps_supplycost", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ps_partkey)),
            Arc::new(Int64Array::from(ps_suppkey)),
            Arc::new(Int64Array::from(ps_availqty)),
            Arc::new(Float64Array::from(ps_supplycost)),
        ],
    )
    .expect("Failed to create partsupp batch");

    conn.register_batches("partsupp", vec![batch])
        .expect("Failed to register partsupp table");
}

// ============================================================================
// Helper: Create a fully-populated TPC-H connection at a given scale factor
// ============================================================================

fn create_tpch_connection(sf: usize) -> Connection {
    let conn = Connection::in_memory().expect("Failed to create connection");
    generate_lineitem(&conn, sf);
    generate_orders(&conn, sf);
    generate_customer(&conn, sf);
    generate_nation(&conn);
    generate_region(&conn);
    generate_supplier(&conn, sf);
    generate_part(&conn, sf);
    generate_partsupp(&conn, sf);
    conn
}

// ============================================================================
// TPC-H Queries (simplified to match Blaze's current SQL capabilities)
// ============================================================================

/// TPC-H Q1 - Pricing Summary Report
///
/// Groups lineitem by returnflag and linestatus, computing aggregate pricing
/// metrics. This is a pure scan + aggregate + sort query (no joins).
const TPCH_Q1: &str = "\
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    COUNT(*) AS count_order
FROM lineitem
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
";

/// TPC-H Q3 - Shipping Priority (simplified)
///
/// Joins lineitem with orders and computes revenue per order.
/// Simplified: omits customer join and date filter from the original.
const TPCH_Q3: &str = "\
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
GROUP BY l_orderkey
ORDER BY revenue DESC
LIMIT 10
";

/// TPC-H Q5 - Local Supplier Volume (simplified)
///
/// Joins customer, orders, lineitem, supplier, and nation to compute
/// revenue by nation. Simplified to a 3-table join.
const TPCH_Q5: &str = "\
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
GROUP BY n_name
ORDER BY revenue DESC
";

/// TPC-H Q6 - Forecasting Revenue Change
///
/// A pure scan + filter + aggregate query on lineitem. No joins.
/// Tests predicate evaluation and simple aggregation performance.
const TPCH_Q6: &str = "\
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
";

/// TPC-H Q10 - Returned Item Reporting (simplified)
///
/// Joins customer with orders and lineitem to find customers with
/// returned items. Simplified: omits date filter.
const TPCH_Q10: &str = "\
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_returnflag = 'R'
GROUP BY c_custkey, c_name
ORDER BY revenue DESC
LIMIT 20
";

/// TPC-H Q12 - Shipping Modes and Order Priority (simplified)
///
/// Groups lineitem by linestatus, counting orders. Simplified from
/// the original which groups by ship mode with CASE expressions.
const TPCH_Q12: &str = "\
SELECT
    l_linestatus,
    COUNT(*) AS order_count
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
GROUP BY l_linestatus
ORDER BY l_linestatus
";

/// TPC-H Q14 - Promotion Effect (simplified)
///
/// Computes overall revenue from lineitem. The original computes
/// promotion revenue as a fraction; simplified here to a filtered aggregate.
const TPCH_Q14: &str = "\
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem
JOIN part ON l_partkey = p_partkey
";

// ============================================================================
// Benchmark Functions
// ============================================================================

fn tpch_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpch");
    // TPC-H benchmarks can be slower; increase measurement time
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(30);

    for sf in [1, 2] {
        let conn = create_tpch_connection(sf);

        // Q1: Pricing Summary Report (scan + aggregate + sort)
        group.bench_with_input(BenchmarkId::new("Q1_pricing_summary", sf), &sf, |b, _| {
            b.iter(|| black_box(conn.query(TPCH_Q1).unwrap()))
        });

        // Q3: Shipping Priority (join + aggregate + sort + limit)
        group.bench_with_input(BenchmarkId::new("Q3_shipping_priority", sf), &sf, |b, _| {
            b.iter(|| black_box(conn.query(TPCH_Q3).unwrap()))
        });

        // Q5: Local Supplier Volume (multi-join + aggregate + sort)
        group.bench_with_input(
            BenchmarkId::new("Q5_local_supplier_volume", sf),
            &sf,
            |b, _| b.iter(|| black_box(conn.query(TPCH_Q5).unwrap())),
        );

        // Q6: Forecasting Revenue Change (scan + filter + aggregate)
        group.bench_with_input(
            BenchmarkId::new("Q6_forecasting_revenue", sf),
            &sf,
            |b, _| b.iter(|| black_box(conn.query(TPCH_Q6).unwrap())),
        );

        // Q10: Returned Item Reporting (multi-join + filter + aggregate + sort + limit)
        group.bench_with_input(
            BenchmarkId::new("Q10_returned_item_reporting", sf),
            &sf,
            |b, _| b.iter(|| black_box(conn.query(TPCH_Q10).unwrap())),
        );

        // Q12: Shipping Modes (join + aggregate + sort)
        group.bench_with_input(BenchmarkId::new("Q12_shipping_modes", sf), &sf, |b, _| {
            b.iter(|| black_box(conn.query(TPCH_Q12).unwrap()))
        });

        // Q14: Promotion Effect (join + aggregate)
        group.bench_with_input(BenchmarkId::new("Q14_promotion_effect", sf), &sf, |b, _| {
            b.iter(|| black_box(conn.query(TPCH_Q14).unwrap()))
        });
    }

    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(benches, tpch_benchmarks);
criterion_main!(benches);
