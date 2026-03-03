# Blaze API Stability Policy

## Versioning

Blaze follows [Semantic Versioning](https://semver.org/):

- **Patch** (0.1.x): Bug fixes, documentation improvements
- **Minor** (0.x.0): New features, non-breaking API additions
- **Major** (x.0.0): Breaking API changes

## Stability Tiers

### Stable API (safe to depend on)

These types and methods are covered by semver guarantees:

- `Connection` — all `pub fn` methods
- `ConnectionConfig` — builder methods
- `BlazeError` / `Result` — error types (note: `#[non_exhaustive]`)
- `ScalarValue` — value representation
- `DataType`, `Schema`, `Field` — type system
- `PreparedStatement` / `PreparedStatementCache` — query preparation
- `ScalarExtract` trait — scalar value extraction

### Experimental API (may change)

Feature-gated modules behind `--features` flags are not covered by semver:
- `adaptive`, `flight`, `federation`, `gpu`, `lakehouse`
- `learned-optimizer`, `nlq`, `simd`, `streaming`, `timeseries`

### Internal API (no stability guarantee)

- Anything in `planner::` (logical/physical plan internals)
- Anything in `executor::` (operator internals)
- Anything in `optimizer::` (optimization rules)

## Error Codes

Error codes (returned by `BlazeError::error_code()`) are stable:

| Code | Category |
|------|----------|
| BP0xx | Parse errors |
| BA0xx | Analysis errors |
| BL0xx | Planning errors |
| BO0xx | Optimization errors |
| BE0xx | Execution errors |
| BT0xx | Type errors |
| BS0xx | Schema errors |
| BC0xx | Catalog errors |
| BI0xx | I/O errors (Arrow, Parquet) |
| BR0xx | Resource errors |
| BN0xx | Not implemented |

## Deprecation Process

1. Add `#[deprecated(since = "X.Y.Z", note = "Use Z instead")]`
2. Maintain deprecated API for at least one minor version
3. Remove in next major version
4. Document in CHANGELOG.md

## Minimum Supported Rust Version (MSRV)

Current MSRV: **1.92.0** (declared in `Cargo.toml` and `rust-toolchain.toml`)

MSRV bumps are treated as breaking changes and noted in the changelog.
