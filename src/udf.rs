//! User-Defined Functions (UDFs)
//!
//! Allows users to register custom scalar and aggregate functions
//! that can be called from SQL queries.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use arrow::array::ArrayRef;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use crate::types::DataType;

/// A user-defined scalar function.
///
/// Wraps a closure that operates on Arrow arrays, allowing custom functions
/// to be called from SQL queries. Register via [`UdfRegistry::register_scalar`].
pub struct ScalarUdf {
    /// Function name (used in SQL).
    pub name: String,
    /// Expected input argument types.
    pub arg_types: Vec<DataType>,
    /// Return type of the function.
    pub return_type: DataType,
    /// The function implementation operating on columnar Arrow arrays.
    pub func: Arc<dyn Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync>,
}

impl ScalarUdf {
    /// Create a new scalar UDF.
    pub fn new(
        name: impl Into<String>,
        arg_types: Vec<DataType>,
        return_type: DataType,
        func: impl Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            arg_types,
            return_type,
            func: Arc::new(func),
        }
    }

    /// Execute the UDF on the given arguments.
    pub fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        // Validate argument count
        if !self.arg_types.is_empty() && args.len() != self.arg_types.len() {
            return Err(BlazeError::execution(format!(
                "UDF '{}' expects {} arguments, got {}",
                self.name,
                self.arg_types.len(),
                args.len()
            )));
        }
        (self.func)(args)
    }
}

impl fmt::Debug for ScalarUdf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarUdf")
            .field("name", &self.name)
            .field("arg_types", &self.arg_types)
            .field("return_type", &self.return_type)
            .finish()
    }
}

/// Registry for user-defined functions.
///
/// Stores scalar UDFs and aggregate UDAFs, keyed by uppercase name.
/// Thread-safe via internal `RwLock` on each map.
#[derive(Default)]
pub struct UdfRegistry {
    scalar_udfs: RwLock<HashMap<String, Arc<ScalarUdf>>>,
    aggregate_udfs: RwLock<HashMap<String, Arc<AggregateUdf>>>,
}

impl UdfRegistry {
    /// Create a new empty UDF registry.
    pub fn new() -> Self {
        Self {
            scalar_udfs: RwLock::new(HashMap::new()),
            aggregate_udfs: RwLock::new(HashMap::new()),
        }
    }

    /// Register a scalar UDF.
    pub fn register_scalar(&self, udf: ScalarUdf) -> Result<()> {
        let name = udf.name.to_uppercase();
        let mut udfs = self.scalar_udfs.write().map_err(|e| {
            BlazeError::internal(format!("Failed to acquire UDF registry lock: {}", e))
        })?;
        udfs.insert(name, Arc::new(udf));
        Ok(())
    }

    /// Get a scalar UDF by name.
    pub fn get_scalar(&self, name: &str) -> Option<Arc<ScalarUdf>> {
        let udfs = self.scalar_udfs.read().ok()?;
        udfs.get(&name.to_uppercase()).cloned()
    }

    /// Check if a scalar UDF exists.
    pub fn has_scalar(&self, name: &str) -> bool {
        self.scalar_udfs
            .read()
            .ok()
            .map(|udfs| udfs.contains_key(&name.to_uppercase()))
            .unwrap_or(false)
    }

    /// List all registered scalar UDF names.
    pub fn list_scalar_udfs(&self) -> Vec<String> {
        self.scalar_udfs
            .read()
            .ok()
            .map(|udfs| udfs.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Deregister a scalar UDF.
    pub fn deregister_scalar(&self, name: &str) -> Result<bool> {
        let mut udfs = self.scalar_udfs.write().map_err(|e| {
            BlazeError::internal(format!("Failed to acquire UDF registry lock: {}", e))
        })?;
        Ok(udfs.remove(&name.to_uppercase()).is_some())
    }

    /// Register a user-defined aggregate function.
    pub fn register_aggregate(&self, udaf: AggregateUdf) -> Result<()> {
        let name = udaf.name.to_uppercase();
        let mut udafs = self.aggregate_udfs.write().map_err(|e| {
            BlazeError::internal(format!("Failed to acquire UDAF registry lock: {}", e))
        })?;
        udafs.insert(name, Arc::new(udaf));
        Ok(())
    }

    /// Get a UDAF by name.
    pub fn get_aggregate(&self, name: &str) -> Option<Arc<AggregateUdf>> {
        let udafs = self.aggregate_udfs.read().ok()?;
        udafs.get(&name.to_uppercase()).cloned()
    }

    /// Check if a UDAF exists.
    pub fn has_aggregate(&self, name: &str) -> bool {
        self.aggregate_udfs
            .read()
            .ok()
            .map(|udafs| udafs.contains_key(&name.to_uppercase()))
            .unwrap_or(false)
    }

    /// List all registered UDAF names.
    pub fn list_aggregate_udfs(&self) -> Vec<String> {
        self.aggregate_udfs
            .read()
            .ok()
            .map(|udafs| udafs.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Deregister a UDAF.
    pub fn deregister_aggregate(&self, name: &str) -> Result<bool> {
        let mut udafs = self.aggregate_udfs.write().map_err(|e| {
            BlazeError::internal(format!("Failed to acquire UDAF registry lock: {}", e))
        })?;
        Ok(udafs.remove(&name.to_uppercase()).is_some())
    }
}

impl fmt::Debug for UdfRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<String> = self.list_scalar_udfs();
        f.debug_struct("UdfRegistry")
            .field("scalar_udfs", &names)
            .finish()
    }
}

/// Physical expression for executing a UDF.
///
/// Bridges the UDF registry with the physical plan by evaluating
/// argument expressions and passing the results to the UDF closure.
#[derive(Debug)]
pub struct UdfExpr {
    udf: Arc<ScalarUdf>,
    args: Vec<Arc<dyn crate::planner::PhysicalExpr>>,
}

impl UdfExpr {
    /// Create a new UDF expression with the given function and argument expressions.
    pub fn new(udf: Arc<ScalarUdf>, args: Vec<Arc<dyn crate::planner::PhysicalExpr>>) -> Self {
        Self { udf, args }
    }
}

impl crate::planner::PhysicalExpr for UdfExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        self.udf.return_type.to_arrow()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let arg_arrays: Result<Vec<ArrayRef>> =
            self.args.iter().map(|arg| arg.evaluate(batch)).collect();
        self.udf.execute(&arg_arrays?)
    }

    fn name(&self) -> &str {
        &self.udf.name
    }
}

/// A trait for user-defined aggregate function accumulators.
///
/// Implementations maintain state across multiple `update` calls
/// and produce a final aggregate result via `finalize`.
pub trait UdafAccumulator: Send + Sync {
    /// Update the accumulator with new input values.
    fn update(&mut self, values: &[ArrayRef]) -> Result<()>;

    /// Merge another accumulator's state into this one (for parallel execution).
    fn merge(&mut self, other: &dyn UdafAccumulator) -> Result<()>;

    /// Produce the final aggregate result as a single-element array.
    fn finalize(&self) -> Result<ArrayRef>;

    /// Reset the accumulator to its initial state.
    fn reset(&mut self);

    /// Get the current state as a boxed Any for downcasting during merge.
    fn state(&self) -> &dyn std::any::Any;
}

/// A user-defined aggregate function.
///
/// Defines the signature and accumulator factory for a custom aggregate.
/// Register via [`UdfRegistry::register_aggregate`].
pub struct AggregateUdf {
    /// Function name (used in SQL).
    pub name: String,
    /// Expected input argument types.
    pub arg_types: Vec<DataType>,
    /// Return type of the aggregate.
    pub return_type: DataType,
    /// Factory to create a new accumulator instance for each group.
    pub accumulator_factory: Arc<dyn Fn() -> Box<dyn UdafAccumulator> + Send + Sync>,
}

impl AggregateUdf {
    /// Create a new aggregate UDF with the given name, types, and accumulator factory.
    pub fn new(
        name: impl Into<String>,
        arg_types: Vec<DataType>,
        return_type: DataType,
        factory: impl Fn() -> Box<dyn UdafAccumulator> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            arg_types,
            return_type,
            accumulator_factory: Arc::new(factory),
        }
    }

    /// Create a new accumulator for this UDAF.
    pub fn create_accumulator(&self) -> Box<dyn UdafAccumulator> {
        (self.accumulator_factory)()
    }
}

impl fmt::Debug for AggregateUdf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateUdf")
            .field("name", &self.name)
            .field("arg_types", &self.arg_types)
            .field("return_type", &self.return_type)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Table-Valued Functions (TVFs)
// ---------------------------------------------------------------------------

/// A table-valued function that returns a set of rows.
///
/// TVFs accept scalar arguments and produce one or more `RecordBatch` results.
/// Register via [`UdfCatalog::register_table_function`].
pub struct TableFunction {
    /// Function name.
    pub name: String,
    /// Human-readable description for the UDF catalog.
    pub description: String,
    /// Expected input argument types.
    pub arg_types: Vec<DataType>,
    /// Output schema (columns produced by this function).
    pub output_schema: crate::types::Schema,
    /// The function implementation: scalar args → `Vec<RecordBatch>`.
    pub func: Arc<dyn Fn(&[crate::types::ScalarValue]) -> Result<Vec<RecordBatch>> + Send + Sync>,
}

impl TableFunction {
    /// Create a new table-valued function.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        arg_types: Vec<DataType>,
        output_schema: crate::types::Schema,
        func: impl Fn(&[crate::types::ScalarValue]) -> Result<Vec<RecordBatch>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            arg_types,
            output_schema,
            func: Arc::new(func),
        }
    }

    /// Execute the TVF with the given arguments.
    pub fn execute(&self, args: &[crate::types::ScalarValue]) -> Result<Vec<RecordBatch>> {
        (self.func)(args)
    }
}

impl fmt::Debug for TableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunction")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("arg_types", &self.arg_types)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// UDF Catalog (discovery and documentation)
// ---------------------------------------------------------------------------

/// Entry in the UDF catalog for documentation and discovery purposes.
#[derive(Debug, Clone)]
pub struct UdfCatalogEntry {
    /// Function name.
    pub name: String,
    /// Kind of UDF (scalar, aggregate, or table).
    pub kind: UdfKind,
    /// Human-readable description.
    pub description: String,
    /// Expected input argument types.
    pub arg_types: Vec<DataType>,
    /// Return type (if applicable; `None` for table functions).
    pub return_type: Option<DataType>,
    /// Optional usage example.
    pub example: Option<String>,
}

/// Kind of UDF.
///
/// Distinguishes scalar, aggregate, and table-valued functions in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfKind {
    /// A scalar function (one output row per input row).
    Scalar,
    /// An aggregate function (many input rows → one output row per group).
    Aggregate,
    /// A table-valued function (returns a set of rows).
    Table,
}

impl fmt::Display for UdfKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UdfKind::Scalar => write!(f, "SCALAR"),
            UdfKind::Aggregate => write!(f, "AGGREGATE"),
            UdfKind::Table => write!(f, "TABLE"),
        }
    }
}

/// Catalog of all registered UDFs for discovery and documentation.
///
/// Provides search, listing, and table-function lookup across all UDF kinds.
pub struct UdfCatalog {
    entries: RwLock<Vec<UdfCatalogEntry>>,
    table_functions: RwLock<HashMap<String, Arc<TableFunction>>>,
}

impl UdfCatalog {
    /// Create a new empty UDF catalog.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            table_functions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a catalog entry.
    pub fn register(&self, entry: UdfCatalogEntry) {
        if let Ok(mut entries) = self.entries.write() {
            entries.push(entry);
        }
    }

    /// Register a table function.
    pub fn register_table_function(&self, tvf: TableFunction) {
        let name = tvf.name.to_uppercase();
        self.register(UdfCatalogEntry {
            name: name.clone(),
            kind: UdfKind::Table,
            description: tvf.description.clone(),
            arg_types: tvf.arg_types.clone(),
            return_type: None,
            example: None,
        });
        if let Ok(mut table_fns) = self.table_functions.write() {
            table_fns.insert(name, Arc::new(tvf));
        }
    }

    /// Get a table function by name.
    pub fn get_table_function(&self, name: &str) -> Option<Arc<TableFunction>> {
        self.table_functions
            .read()
            .ok()?
            .get(&name.to_uppercase())
            .cloned()
    }

    /// List all catalog entries.
    pub fn list(&self) -> Vec<UdfCatalogEntry> {
        self.entries.read().map(|e| e.clone()).unwrap_or_default()
    }

    /// Search catalog by name pattern (case-insensitive substring).
    pub fn search(&self, pattern: &str) -> Vec<UdfCatalogEntry> {
        let pattern = pattern.to_uppercase();
        self.entries
            .read()
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| e.name.contains(&pattern))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List entries by kind.
    pub fn list_by_kind(&self, kind: UdfKind) -> Vec<UdfCatalogEntry> {
        self.entries
            .read()
            .map(|entries| entries.iter().filter(|e| e.kind == kind).cloned().collect())
            .unwrap_or_default()
    }

    /// Total number of registered functions across all kinds.
    pub fn count(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }
}

impl Default for UdfCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array};

    #[test]
    fn test_scalar_udf_registration() {
        let registry = UdfRegistry::new();

        let double_fn = ScalarUdf::new(
            "double",
            vec![DataType::Int64],
            DataType::Int64,
            |args: &[ArrayRef]| {
                let input = args[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64Array"))?;
                let result: Int64Array = input.iter().map(|v| v.map(|x| x * 2)).collect();
                Ok(Arc::new(result) as ArrayRef)
            },
        );

        registry.register_scalar(double_fn).unwrap();
        assert!(registry.has_scalar("DOUBLE"));
        assert!(registry.has_scalar("double"));

        let udf = registry.get_scalar("double").unwrap();
        let input = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = udf.execute(&[input]).unwrap();
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.value(0), 2);
        assert_eq!(result.value(1), 4);
        assert_eq!(result.value(2), 6);
    }

    #[test]
    fn test_udf_deregister() {
        let registry = UdfRegistry::new();

        let noop = ScalarUdf::new("noop", vec![], DataType::Int64, |_| {
            Ok(Arc::new(Int64Array::from(vec![0])) as ArrayRef)
        });

        registry.register_scalar(noop).unwrap();
        assert!(registry.has_scalar("NOOP"));

        registry.deregister_scalar("noop").unwrap();
        assert!(!registry.has_scalar("NOOP"));
    }

    #[test]
    fn test_list_udfs() {
        let registry = UdfRegistry::new();

        let f1 = ScalarUdf::new("func_a", vec![], DataType::Int64, |_| {
            Ok(Arc::new(Int64Array::from(vec![0])) as ArrayRef)
        });
        let f2 = ScalarUdf::new("func_b", vec![], DataType::Int64, |_| {
            Ok(Arc::new(Int64Array::from(vec![0])) as ArrayRef)
        });

        registry.register_scalar(f1).unwrap();
        registry.register_scalar(f2).unwrap();

        let names = registry.list_scalar_udfs();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"FUNC_A".to_string()));
        assert!(names.contains(&"FUNC_B".to_string()));
    }

    /// Example: geometric mean UDAF
    struct GeometricMeanAccumulator {
        log_sum: f64,
        count: u64,
    }

    impl GeometricMeanAccumulator {
        fn boxed() -> Box<dyn UdafAccumulator> {
            Box::new(Self {
                log_sum: 0.0,
                count: 0,
            })
        }
    }

    impl UdafAccumulator for GeometricMeanAccumulator {
        fn update(&mut self, values: &[ArrayRef]) -> Result<()> {
            let arr = values[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| BlazeError::type_error("Expected Float64Array"))?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let v = arr.value(i);
                    if v > 0.0 {
                        self.log_sum += v.ln();
                        self.count += 1;
                    }
                }
            }
            Ok(())
        }

        fn merge(&mut self, other: &dyn UdafAccumulator) -> Result<()> {
            let other = other
                .state()
                .downcast_ref::<GeometricMeanAccumulator>()
                .ok_or_else(|| BlazeError::internal("State type mismatch in merge"))?;
            self.log_sum += other.log_sum;
            self.count += other.count;
            Ok(())
        }

        fn finalize(&self) -> Result<ArrayRef> {
            let result = if self.count == 0 {
                f64::NAN
            } else {
                (self.log_sum / self.count as f64).exp()
            };
            Ok(Arc::new(Float64Array::from(vec![result])) as ArrayRef)
        }

        fn reset(&mut self) {
            self.log_sum = 0.0;
            self.count = 0;
        }

        fn state(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_udaf_registration() {
        let registry = UdfRegistry::new();
        let udaf = AggregateUdf::new(
            "geo_mean",
            vec![DataType::Float64],
            DataType::Float64,
            GeometricMeanAccumulator::boxed,
        );
        registry.register_aggregate(udaf).unwrap();
        assert!(registry.has_aggregate("geo_mean"));
        assert!(registry.has_aggregate("GEO_MEAN"));
    }

    #[test]
    fn test_udaf_accumulator_lifecycle() {
        let udaf = AggregateUdf::new(
            "geo_mean",
            vec![DataType::Float64],
            DataType::Float64,
            GeometricMeanAccumulator::boxed,
        );

        let mut acc = udaf.create_accumulator();

        // Update with some values
        let values = Arc::new(Float64Array::from(vec![2.0, 8.0])) as ArrayRef;
        acc.update(&[values]).unwrap();

        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        // Geometric mean of [2, 8] = (2*8)^0.5 = 4
        assert!((arr.value(0) - 4.0).abs() < 0.01);
    }

    #[test]
    fn test_udaf_accumulator_reset() {
        let udaf = AggregateUdf::new(
            "geo_mean",
            vec![DataType::Float64],
            DataType::Float64,
            GeometricMeanAccumulator::boxed,
        );

        let mut acc = udaf.create_accumulator();
        let values = Arc::new(Float64Array::from(vec![4.0])) as ArrayRef;
        acc.update(&[values]).unwrap();
        acc.reset();

        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.value(0).is_nan());
    }

    #[test]
    fn test_udaf_list_and_deregister() {
        let registry = UdfRegistry::new();
        let udaf1 = AggregateUdf::new(
            "agg_a",
            vec![],
            DataType::Float64,
            GeometricMeanAccumulator::boxed,
        );
        let udaf2 = AggregateUdf::new(
            "agg_b",
            vec![],
            DataType::Float64,
            GeometricMeanAccumulator::boxed,
        );

        registry.register_aggregate(udaf1).unwrap();
        registry.register_aggregate(udaf2).unwrap();

        let names = registry.list_aggregate_udfs();
        assert_eq!(names.len(), 2);

        registry.deregister_aggregate("agg_a").unwrap();
        assert!(!registry.has_aggregate("agg_a"));
        assert_eq!(registry.list_aggregate_udfs().len(), 1);
    }

    // -- Table-Valued Function tests ------------------------------------------

    #[test]
    fn test_table_function() {
        let schema =
            crate::types::Schema::new(vec![crate::types::Field::new("n", DataType::Int64, false)]);
        let tvf = TableFunction::new(
            "generate_series",
            "Generate a series of integers",
            vec![DataType::Int64],
            schema,
            |args| {
                let count = match &args[0] {
                    crate::types::ScalarValue::Int64(Some(n)) => *n,
                    _ => 10,
                };
                let arr = Arc::new(Int64Array::from((0..count).collect::<Vec<_>>()));
                let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
                ]));
                Ok(vec![RecordBatch::try_new(arrow_schema, vec![arr])?])
            },
        );

        let result = tvf
            .execute(&[crate::types::ScalarValue::Int64(Some(5))])
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5);
    }

    // -- UDF Catalog tests ----------------------------------------------------

    #[test]
    fn test_udf_catalog_register_and_search() {
        let catalog = UdfCatalog::new();
        catalog.register(UdfCatalogEntry {
            name: "MY_FUNC".into(),
            kind: UdfKind::Scalar,
            description: "A test function".into(),
            arg_types: vec![DataType::Int64],
            return_type: Some(DataType::Int64),
            example: Some("SELECT MY_FUNC(42)".into()),
        });
        catalog.register(UdfCatalogEntry {
            name: "OTHER_FUNC".into(),
            kind: UdfKind::Aggregate,
            description: "Another function".into(),
            arg_types: vec![],
            return_type: Some(DataType::Float64),
            example: None,
        });

        assert_eq!(catalog.count(), 2);
        assert_eq!(catalog.search("MY").len(), 1);
        assert_eq!(catalog.list_by_kind(UdfKind::Scalar).len(), 1);
        assert_eq!(catalog.list_by_kind(UdfKind::Aggregate).len(), 1);
    }

    #[test]
    fn test_udf_catalog_table_function() {
        let catalog = UdfCatalog::new();
        let schema =
            crate::types::Schema::new(vec![crate::types::Field::new("x", DataType::Int64, false)]);
        let tvf = TableFunction::new("gen", "Generate values", vec![], schema, |_| Ok(Vec::new()));
        catalog.register_table_function(tvf);

        assert_eq!(catalog.count(), 1);
        assert!(catalog.get_table_function("gen").is_some());
        assert!(catalog.get_table_function("GEN").is_some());
    }

    #[test]
    fn test_udf_kind_display() {
        assert_eq!(format!("{}", UdfKind::Scalar), "SCALAR");
        assert_eq!(format!("{}", UdfKind::Aggregate), "AGGREGATE");
        assert_eq!(format!("{}", UdfKind::Table), "TABLE");
    }

    // --- Error case tests ---

    #[test]
    fn test_scalar_udf_wrong_arg_count() {
        let udf = ScalarUdf::new(
            "add",
            vec![DataType::Int64, DataType::Int64],
            DataType::Int64,
            |args: &[ArrayRef]| {
                let a = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
                let b = args[1].as_any().downcast_ref::<Int64Array>().unwrap();
                let result: Int64Array = a
                    .iter()
                    .zip(b.iter())
                    .map(|(a, b)| match (a, b) {
                        (Some(a), Some(b)) => Some(a + b),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(result) as ArrayRef)
            },
        );

        // Call with wrong number of args
        let input = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = udf.execute(&[input]);
        assert!(result.is_err(), "Should error with wrong argument count");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("expects 2 arguments, got 1"), "Error: {}", msg);
    }

    #[test]
    fn test_scalar_udf_wrong_arg_type() {
        let udf = ScalarUdf::new(
            "double",
            vec![DataType::Int64],
            DataType::Int64,
            |args: &[ArrayRef]| {
                let input = args[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64Array"))?;
                let result: Int64Array = input.iter().map(|v| v.map(|x| x * 2)).collect();
                Ok(Arc::new(result) as ArrayRef)
            },
        );

        // Pass a Float64 instead of Int64
        let input = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
        let result = udf.execute(&[input]);
        assert!(result.is_err(), "Should error with wrong type");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Int64"),
            "Error should mention expected type: {}",
            msg
        );
    }

    #[test]
    fn test_scalar_udf_null_inputs() {
        let udf = ScalarUdf::new(
            "double",
            vec![DataType::Int64],
            DataType::Int64,
            |args: &[ArrayRef]| {
                let input = args[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64Array"))?;
                let result: Int64Array = input.iter().map(|v| v.map(|x| x * 2)).collect();
                Ok(Arc::new(result) as ArrayRef)
            },
        );

        // Input with NULL values
        let input = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let result = udf.execute(&[input]).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 2);
        assert!(arr.is_null(1), "NULL input should produce NULL output");
        assert_eq!(arr.value(2), 6);
    }

    #[test]
    fn test_scalar_udf_empty_args_with_defined_types() {
        let udf = ScalarUdf::new(
            "needs_one",
            vec![DataType::Int64],
            DataType::Int64,
            |_args: &[ArrayRef]| Ok(Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        );

        let result = udf.execute(&[]);
        assert!(
            result.is_err(),
            "Should error with zero args when one expected"
        );
    }

    #[test]
    fn test_scalar_udf_variadic_empty_args() {
        // UDF with no declared arg types (variadic)
        let udf = ScalarUdf::new("noop", vec![], DataType::Int64, |_args: &[ArrayRef]| {
            Ok(Arc::new(Int64Array::from(vec![42])) as ArrayRef)
        });

        // Should succeed with any number of args when arg_types is empty
        let result = udf.execute(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_udaf_accumulator_wrong_type_merge() {
        let udaf1 = AggregateUdf::new(
            "geo_mean",
            vec![DataType::Float64],
            DataType::Float64,
            GeometricMeanAccumulator::boxed,
        );
        let udaf2 = AggregateUdf::new(
            "geo_mean2",
            vec![DataType::Float64],
            DataType::Float64,
            GeometricMeanAccumulator::boxed,
        );

        let mut acc1 = udaf1.create_accumulator();
        let acc2 = udaf2.create_accumulator();

        // Merge should succeed since both are GeometricMeanAccumulator
        let result = acc1.merge(acc2.as_ref());
        assert!(result.is_ok());
    }

    #[test]
    fn test_udf_deregister_nonexistent() {
        let registry = UdfRegistry::new();
        let removed = registry.deregister_scalar("nonexistent").unwrap();
        assert!(
            !removed,
            "Deregistering nonexistent UDF should return false"
        );
    }

    #[test]
    fn test_udaf_deregister_nonexistent() {
        let registry = UdfRegistry::new();
        let removed = registry.deregister_aggregate("nonexistent").unwrap();
        assert!(
            !removed,
            "Deregistering nonexistent UDAF should return false"
        );
    }

    #[test]
    fn test_udf_get_nonexistent() {
        let registry = UdfRegistry::new();
        assert!(registry.get_scalar("nonexistent").is_none());
        assert!(registry.get_aggregate("nonexistent").is_none());
    }

    #[test]
    fn test_scalar_udf_debug() {
        let udf = ScalarUdf::new("test_fn", vec![DataType::Int64], DataType::Int64, |_| {
            Ok(Arc::new(Int64Array::from(vec![0])) as ArrayRef)
        });
        let debug = format!("{:?}", udf);
        assert!(debug.contains("test_fn"));
        assert!(debug.contains("ScalarUdf"));
    }

    #[test]
    fn test_udf_catalog_search_no_match() {
        let catalog = UdfCatalog::new();
        catalog.register(UdfCatalogEntry {
            name: "MY_FUNC".into(),
            kind: UdfKind::Scalar,
            description: "A test function".into(),
            arg_types: vec![],
            return_type: Some(DataType::Int64),
            example: None,
        });
        let results = catalog.search("NONEXISTENT");
        assert!(results.is_empty());
    }

    #[test]
    fn test_udf_catalog_empty() {
        let catalog = UdfCatalog::new();
        assert_eq!(catalog.count(), 0);
        assert!(catalog.list().is_empty());
        assert!(catalog.get_table_function("anything").is_none());
    }
}
