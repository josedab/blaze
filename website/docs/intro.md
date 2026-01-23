---
sidebar_position: 1
slug: /
---

# Introduction

Blaze is a high-performance, memory-safe embedded OLAP (Online Analytical Processing) query engine written in Rust. It provides SQL:2016 compliance with native Apache Arrow and Parquet integration.

## What is Blaze?

Blaze is an **embedded database** designed for analytical workloads. Unlike traditional databases that run as separate server processes, Blaze runs directly inside your application with zero network overhead.

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    // Create an in-memory database - no server needed
    let conn = Connection::in_memory()?;

    // Query Parquet files directly
    conn.register_parquet("sales", "data/sales.parquet")?;

    let results = conn.query("
        SELECT product, SUM(amount) as total
        FROM sales
        GROUP BY product
        ORDER BY total DESC
        LIMIT 10
    ")?;

    Ok(())
}
```

## Key Features

### Memory Safety

Built entirely in Rust, Blaze guarantees memory safety at compile time. No buffer overflows, no use-after-free bugs, no data races.

### Vectorized Execution

Processes data in batches (default 8,192 rows) using Apache Arrow's columnar format. SIMD-optimized operations deliver maximum throughput.

### Full SQL Support

SQL:2016 compliant with support for:

- **Queries**: SELECT, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT
- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, SEMI, ANTI
- **Aggregations**: COUNT, SUM, AVG, MIN, MAX
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD
- **Advanced**: CTEs, subqueries, UNION, INTERSECT, EXCEPT

### Native Arrow Integration

First-class Apache Arrow support enables:

- Zero-copy data sharing between components
- Seamless interoperability with PyArrow, Pandas, Polars
- Efficient columnar processing

### Multiple Storage Formats

Query data wherever it lives:

- **Parquet**: Columnar format for analytics
- **CSV**: Simple text format
- **Delta Lake**: ACID transactions and time travel
- **In-memory**: Fast ephemeral tables

## When to Use Blaze

Blaze is ideal for:

| Use Case | Why Blaze |
|----------|-----------|
| **Embedded Analytics** | Add SQL capabilities to your app without external dependencies |
| **ETL Pipelines** | Process Parquet/CSV files with familiar SQL syntax |
| **Data Science** | Fast local queries via Python bindings |
| **CLI Tools** | Build data processing tools with embedded SQL |
| **Edge Computing** | Lightweight footprint for resource-constrained environments |

## When Not to Use Blaze

Blaze may not be the best choice for:

- **OLTP workloads**: High-frequency single-row transactions
- **Multi-user servers**: Blaze is single-process, not a server
- **Petabyte-scale data**: Designed for in-memory analytics

## Getting Started

Ready to try Blaze? Head to the [Quick Start](/docs/getting-started/quick-start) guide to run your first query in under 5 minutes.

```bash
cargo add blaze
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Your Application                         │
├─────────────────────────────────────────────────────────────┤
│                        Blaze                                 │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Parser  │→ │ Binder  │→ │Optimizer │→ │Physical Planner│  │
│  └─────────┘  └─────────┘  └──────────┘  └───────────────┘  │
│                                                 ↓            │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                      Executor                           │ │
│  │    (Vectorized, SIMD-optimized, Arrow-native)          │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              ↓                               │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Memory  │  │   CSV   │  │ Parquet  │  │  Delta Lake   │  │
│  └─────────┘  └─────────┘  └──────────┘  └───────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Community

- **GitHub**: [github.com/blaze-db/blaze](https://github.com/blaze-db/blaze)
- **Discussions**: [GitHub Discussions](https://github.com/blaze-db/blaze/discussions)
- **Issues**: [Report bugs or request features](https://github.com/blaze-db/blaze/issues)
