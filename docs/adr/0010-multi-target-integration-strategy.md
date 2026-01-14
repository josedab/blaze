# ADR-0010: Multi-Target Integration Strategy (Python, WASM, GPU)

## Status

Accepted

## Context

A query engine's value increases with the number of environments where it can run. Potential integration targets include:

**Language Bindings**:
- Python: Dominant in data science
- JavaScript/TypeScript: Web and Node.js
- Java/Scala: Enterprise and Spark ecosystem
- Go: Cloud-native applications

**Runtime Targets**:
- Native: Maximum performance
- WebAssembly: Browser and edge
- Mobile: iOS/Android applications

**Accelerators**:
- GPU: Parallel processing for large datasets
- FPGA: Custom acceleration (specialized)

Each target has different requirements, trade-offs, and integration complexity.

## Decision

We implemented a **layered multi-target strategy** prioritizing targets by ecosystem impact:

### Priority 1: Python Bindings (PyBlaze)

Python is the primary data science language. We use PyO3 for zero-overhead bindings:

```rust
// src/python.rs
use pyo3::prelude::*;

#[pyclass]
pub struct PyConnection {
    inner: Arc<Connection>,
}

#[pymethods]
impl PyConnection {
    #[new]
    fn new() -> PyResult<Self> {
        let conn = Connection::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(Self { inner: Arc::new(conn) })
    }

    fn query(&self, sql: &str) -> PyResult<PyQueryResult> {
        let batches = self.inner.query(sql)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(PyQueryResult::new(batches))
    }
}
```

**PyArrow Integration** for zero-copy data exchange:

```rust
#[pymethods]
impl PyQueryResult {
    fn to_pyarrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        // Convert Arrow RecordBatch to PyArrow via Arrow IPC
        let pyarrow = py.import_bound("pyarrow")?;

        // Serialize to IPC format
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &self.schema)?;
            for batch in &self.batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }

        // PyArrow reads IPC directly
        let reader = pyarrow.call_method1(
            "ipc.open_stream",
            (PyBytes::new_bound(py, &buffer),)
        )?;
        reader.call_method0("read_all")
    }

    fn to_polars(&self, py: Python<'_>) -> PyResult<PyObject> {
        let arrow_table = self.to_pyarrow(py)?;
        let polars = py.import_bound("polars")?;
        polars.call_method1("from_arrow", (arrow_table,))
    }

    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        let arrow_table = self.to_pyarrow(py)?;
        arrow_table.call_method0("to_pandas")
    }
}
```

### Priority 2: WebAssembly Target

Browser-based analytics without server round-trips:

```rust
// src/wasm.rs (future)
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmConnection {
    inner: Connection,
}

#[wasm_bindgen]
impl WasmConnection {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WasmConnection, JsValue> {
        let conn = Connection::new()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(Self { inner: conn })
    }

    pub fn query(&self, sql: &str) -> Result<JsValue, JsValue> {
        let batches = self.inner.query(sql)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Convert to JavaScript-friendly format
        self.batches_to_js(&batches)
    }
}
```

### Priority 3: GPU Acceleration

For compute-intensive operations on large datasets:

```rust
// src/gpu/ (foundation exists)
pub mod kernels;
pub mod memory;

/// GPU-accelerated physical operators
pub enum GpuPhysicalPlan {
    GpuScan { /* ... */ },
    GpuFilter { kernel: FilterKernel, /* ... */ },
    GpuHashJoin { /* ... */ },
    GpuAggregate { /* ... */ },
}

/// GPU kernel for filter operations
pub struct FilterKernel {
    shader: wgpu::ShaderModule,
    pipeline: wgpu::ComputePipeline,
}
```

### Feature Flag Architecture

Targets are opt-in via Cargo features:

```toml
# Cargo.toml
[features]
default = []
python = ["pyo3"]
wasm = ["wasm-bindgen", "js-sys"]
gpu = ["wgpu", "gpu-allocator"]

[dependencies]
pyo3 = { version = "0.22", optional = true }
wasm-bindgen = { version = "0.2", optional = true }
wgpu = { version = "0.19", optional = true }
```

```rust
// src/lib.rs
#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "gpu")]
pub mod gpu;
```

## Consequences

### Benefits

1. **Incremental Adoption**: Features compile only when needed:
   ```bash
   # Minimal build
   cargo build

   # With Python bindings
   cargo build --features python

   # All features
   cargo build --all-features
   ```

2. **Shared Core Logic**: All targets use same query engine:
   ```
   ┌─────────────┬─────────────┬─────────────┐
   │   Python    │    WASM     │     GPU     │
   │  (PyO3)     │(wasm-bind)  │   (wgpu)    │
   └──────┬──────┴──────┬──────┴──────┬──────┘
          │             │             │
          └─────────────┼─────────────┘
                        │
              ┌─────────┴─────────┐
              │   Core Engine     │
              │  (Pure Rust)      │
              └───────────────────┘
   ```

3. **Zero-Copy Where Possible**: Arrow format enables efficient data transfer:
   ```python
   # Python: Zero-copy Arrow to Pandas (when dtypes match)
   result = conn.query("SELECT int_col FROM t")
   df = result.to_pandas()  # No data copy for numeric columns
   ```

4. **Native Performance**: Rust code runs at full speed in all targets:
   ```
   Native Rust:  100% performance (baseline)
   Python/PyO3:  ~99% (thin wrapper overhead)
   WASM:         ~70-90% (browser VM overhead)
   GPU:          10-100x for parallel workloads
   ```

5. **Single Codebase**: Bug fixes and features benefit all targets.

### Costs

1. **Build Complexity**: Multiple feature combinations to test:
   ```bash
   # CI matrix
   cargo test                           # core
   cargo test --features python         # + python
   cargo test --features wasm           # + wasm
   cargo test --all-features           # all
   ```

2. **API Surface Per Target**: Each target needs idiomatic bindings:
   ```rust
   // Rust: Result<Vec<RecordBatch>>
   // Python: -> PyQueryResult (with to_pandas(), to_polars())
   // WASM: -> JsValue (JavaScript array)
   ```

3. **Target-Specific Limitations**:
   - WASM: No filesystem access (data must be passed in)
   - GPU: Requires compatible hardware
   - Python: GIL considerations for threading

### Python Integration Details

**Package Structure**:
```
python/
├── pyblaze/
│   ├── __init__.py      # Python interface
│   └── __init__.pyi     # Type stubs for IDE support
└── tests/
    └── test_pyblaze.py  # Integration tests
```

**Build with Maturin**:
```toml
# pyproject.toml
[build-system]
requires = ["maturin>=1.0"]
build-backend = "maturin"

[project]
name = "pyblaze"
requires-python = ">=3.8"
dependencies = ["pyarrow>=10.0.0"]
```

```bash
# Build wheel
maturin build --features python

# Develop mode (editable install)
maturin develop --features python
```

**Usage Example**:
```python
import pyblaze
import pandas as pd

# Connect
conn = pyblaze.connect()

# Register data sources
conn.register_parquet("sales", "s3://bucket/sales.parquet")
conn.register_csv("products", "products.csv")

# Query with SQL
result = conn.query("""
    SELECT p.name, SUM(s.amount) as total
    FROM sales s
    JOIN products p ON s.product_id = p.id
    GROUP BY p.name
    ORDER BY total DESC
    LIMIT 10
""")

# Convert to pandas
df = result.to_pandas()
print(df)

# Or use with Polars
import polars as pl
lf = result.to_polars()
```

### WebAssembly Integration (Planned)

**Browser Usage**:
```javascript
import init, { Connection } from 'blaze-wasm';

async function analyzeData(csvData) {
    await init();

    const conn = new Connection();
    conn.registerCsvString('data', csvData);

    const result = conn.query(`
        SELECT category, AVG(price) as avg_price
        FROM data
        GROUP BY category
    `);

    return result.toArray();
}
```

### GPU Acceleration (Experimental)

**Directory Structure**:
```
src/gpu/
├── mod.rs           # GPU module exports
├── kernels/         # Compute shaders
│   ├── filter.wgsl
│   ├── aggregate.wgsl
│   └── join.wgsl
└── memory.rs        # GPU memory management
```

**Operator Offloading**:
```rust
// Physical planner decides CPU vs GPU
fn plan_filter(&self, input: PhysicalPlan, predicate: Expr) -> PhysicalPlan {
    let input_rows = self.estimate_rows(&input);

    if self.gpu_available() && input_rows > GPU_THRESHOLD {
        PhysicalPlan::GpuFilter { input, predicate }
    } else {
        PhysicalPlan::Filter { input, predicate }
    }
}
```

### Target Comparison

| Aspect | Python | WASM | GPU |
|--------|--------|------|-----|
| Primary use | Data science | Browser | Large data |
| Data exchange | Arrow IPC | JSON/Arrow | GPU memory |
| Performance | Near-native | 70-90% | 10-100x |
| Maturity | Production | Planned | Experimental |
| Dependencies | PyO3, maturin | wasm-bindgen | wgpu |
