# ADR-0001: Rust as Implementation Language

## Status

Accepted

## Context

When designing a new query engine, the choice of implementation language fundamentally shapes the system's capabilities, performance characteristics, and development experience. Query engines have traditionally been implemented in:

- **C/C++**: Maximum performance but prone to memory safety issues (buffer overflows, use-after-free, data races). Examples: SQLite, DuckDB, ClickHouse.
- **Java/Scala**: Memory safety via garbage collection but with GC pause unpredictability and higher memory overhead. Examples: Apache Spark, Presto/Trino.
- **Go**: Good concurrency primitives but GC pauses and less control over memory layout.
- **Rust**: Memory safety without garbage collection, zero-cost abstractions, and modern tooling.

Key requirements for Blaze:

1. **Performance**: OLAP workloads demand efficient memory access patterns and vectorized operations
2. **Memory Safety**: Query engines handle untrusted input (SQL) and must not have exploitable memory bugs
3. **Predictable Latency**: Embedded use cases cannot tolerate GC pauses
4. **Ecosystem Integration**: Must interoperate with Apache Arrow, Python data science stack
5. **Developer Productivity**: Modern tooling, package management, testing infrastructure

## Decision

We chose **Rust** as the implementation language for Blaze.

Rust provides:

- **Memory safety guarantees** enforced at compile time through ownership and borrowing
- **Zero-cost abstractions** allowing high-level code without runtime overhead
- **No garbage collector** eliminating unpredictable pause times
- **Excellent Arrow ecosystem** with official `arrow-rs` crate maintained by Apache
- **Modern tooling** (Cargo, rustfmt, clippy, built-in testing)
- **Strong FFI story** for Python (PyO3), WebAssembly (wasm-bindgen), and C interop
- **Fearless concurrency** through the type system preventing data races

## Consequences

### Benefits

1. **Eliminated Memory Bug Classes**: Buffer overflows, use-after-free, double-free, and data races are caught at compile time. This is critical for a query engine processing arbitrary user SQL.

2. **Predictable Performance**: No GC pauses means consistent query latency. Important for embedded use cases where Blaze runs in the same process as the application.

3. **High Performance**: Rust compiles to native code with LLVM optimizations. Combined with control over memory layout, achieves C-level performance.

4. **Ecosystem Alignment**: The Apache Arrow project maintains `arrow-rs`, ensuring first-class Rust support. PyO3 enables seamless Python bindings.

5. **Safer Concurrency**: Rust's ownership system catches data races at compile time, enabling confident parallel query execution.

6. **Modern Development Experience**: Cargo provides excellent dependency management, `cargo test` for testing, `cargo bench` for benchmarks, and `cargo doc` for documentation.

### Costs

1. **Steeper Learning Curve**: Rust's ownership model requires developers to learn new concepts (lifetimes, borrowing). Team ramp-up time is longer than with Java or Python.

2. **Longer Compile Times**: Rust's extensive compile-time checks and LLVM backend result in longer build times compared to Go or interpreted languages.

3. **Smaller Talent Pool**: Fewer Rust developers exist compared to Java, Python, or C++ developers, potentially affecting hiring.

4. **Library Maturity**: While improving rapidly, some Rust libraries are younger than C++ or Java equivalents.

### Implications

- All core Blaze code is written in Rust
- Python bindings use PyO3 (Rust-native Python FFI)
- WebAssembly support compiles directly from Rust via `wasm-pack`
- Error handling uses Rust's `Result<T, E>` pattern throughout
- Unsafe code is minimized and isolated (currently only in FFI boundaries)
- CI enforces `cargo clippy` and `cargo fmt` for code quality
