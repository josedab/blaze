//! Expression JIT Compilation & Fusion
//!
//! Compiles expression chains into fused evaluation pipelines that avoid
//! intermediate materialization between operators.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;

use crate::error::{BlazeError, Result};
use crate::types::DataType;

/// Compilation tier for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilationTier {
    /// Default: evaluate normally.
    Interpreted,
    /// Fused: multiple exprs evaluated in single pass.
    Fused,
    /// Type-specialized fast paths.
    Specialized,
}

/// A compiled expression that fuses multiple sub-expressions.
pub struct CompiledExpression {
    pub name: String,
    pub tier: CompilationTier,
    pub input_types: Vec<DataType>,
    pub output_type: DataType,
    evaluator: Box<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>,
    compilation_time_us: u64,
    invocation_count: AtomicU64,
}

impl std::fmt::Debug for CompiledExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledExpression")
            .field("name", &self.name)
            .field("tier", &self.tier)
            .field("input_types", &self.input_types)
            .field("output_type", &self.output_type)
            .field("compilation_time_us", &self.compilation_time_us)
            .field("invocation_count", &self.invocation_count)
            .finish()
    }
}

impl CompiledExpression {
    /// Execute the compiled expression against a record batch.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.invocation_count.fetch_add(1, Ordering::Relaxed);
        (self.evaluator)(batch)
    }

    /// Get the number of times this expression has been invoked.
    pub fn invocation_count(&self) -> u64 {
        self.invocation_count.load(Ordering::Relaxed)
    }

    /// Get the compilation time in microseconds.
    pub fn compilation_time_us(&self) -> u64 {
        self.compilation_time_us
    }
}

/// Compilation statistics for profiling.
#[derive(Debug, Clone)]
pub struct CompilationStats {
    pub total_compilations: u64,
    pub total_fusions: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub avg_speedup: f64,
}

impl Default for CompilationStats {
    fn default() -> Self {
        Self {
            total_compilations: 0,
            total_fusions: 0,
            cache_hits: 0,
            cache_misses: 0,
            avg_speedup: 0.0,
        }
    }
}

/// Cache for compiled expressions keyed by expression fingerprint.
pub struct ExpressionCompilationCache {
    entries: RwLock<HashMap<String, Arc<CompiledExpression>>>,
    config: CompilationConfig,
    stats: RwLock<CompilationStats>,
}

impl ExpressionCompilationCache {
    /// Create a new cache with the given configuration.
    pub fn new(config: CompilationConfig) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            config,
            stats: RwLock::new(CompilationStats::default()),
        }
    }

    /// Look up a compiled expression by fingerprint, compiling on cache miss.
    pub fn get_or_compile<F>(
        &self,
        fingerprint: &str,
        compile_fn: F,
    ) -> Result<Arc<CompiledExpression>>
    where
        F: FnOnce() -> Result<CompiledExpression>,
    {
        // Fast path: read lock for cache hit.
        {
            let entries = self.entries.read();
            if let Some(compiled) = entries.get(fingerprint) {
                self.stats.write().cache_hits += 1;
                return Ok(Arc::clone(compiled));
            }
        }

        // Slow path: compile and insert.
        self.stats.write().cache_misses += 1;
        let compiled = Arc::new(compile_fn()?);

        let mut entries = self.entries.write();
        // Evict oldest entries if cache is full.
        if entries.len() >= self.config.max_cache_entries {
            if let Some(key) = entries.keys().next().cloned() {
                entries.remove(&key);
            }
        }
        entries.insert(fingerprint.to_string(), Arc::clone(&compiled));
        self.stats.write().total_compilations += 1;

        Ok(compiled)
    }

    /// Get a snapshot of current compilation statistics.
    pub fn stats(&self) -> CompilationStats {
        self.stats.read().clone()
    }
}

/// Configuration for the JIT compiler.
#[derive(Debug, Clone)]
pub struct CompilationConfig {
    /// Whether JIT compilation is enabled.
    pub enabled: bool,
    /// Compile after this many invocations.
    pub compilation_threshold: u64,
    /// Maximum number of cached compiled expressions.
    pub max_cache_entries: usize,
    /// Enable expression fusion.
    pub enable_fusion: bool,
    /// Enable type-specialized fast paths.
    pub enable_specialization: bool,
}

impl Default for CompilationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            compilation_threshold: 100,
            max_cache_entries: 1024,
            enable_fusion: true,
            enable_specialization: true,
        }
    }
}

/// The expression compiler/fuser.
pub struct ExpressionCompiler {
    cache: Arc<ExpressionCompilationCache>,
    config: CompilationConfig,
}

impl ExpressionCompiler {
    /// Create a new expression compiler with the given configuration.
    pub fn new(config: CompilationConfig) -> Self {
        let cache = Arc::new(ExpressionCompilationCache::new(config.clone()));
        Self { cache, config }
    }

    /// Fuse a filter expression and projection expressions into a single pass.
    ///
    /// The returned compiled expression:
    /// 1. Evaluates the filter predicate to produce a boolean mask
    /// 2. Applies the mask via `filter_record_batch` to avoid intermediate materialization
    /// 3. Evaluates projection expressions only on the filtered rows
    pub fn compile_filter_project(
        &self,
        filter_expr: Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>,
        projection_exprs: Vec<Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>>,
        output_type: DataType,
    ) -> Result<Arc<CompiledExpression>> {
        if !self.config.enabled {
            return Err(BlazeError::execution("JIT compilation is disabled"));
        }

        let fingerprint = format!("filter_project_{}", projection_exprs.len());

        self.cache.get_or_compile(&fingerprint, move || {
            let start = Instant::now();
            let filter = Arc::clone(&filter_expr);
            let projections = projection_exprs;

            let evaluator: Box<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync> =
                Box::new(move |batch: &RecordBatch| {
                    // Step 1: Evaluate filter predicate.
                    let mask_array = filter(batch)?;
                    let mask = mask_array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            BlazeError::execution("Filter expression must return BooleanArray")
                        })?;

                    // Step 2: Apply filter to get filtered batch.
                    let filtered_batch = filter_record_batch(batch, mask)?;

                    // Step 3: Evaluate projections on filtered rows only.
                    if projections.is_empty() {
                        return Ok(Arc::new(mask.clone()) as ArrayRef);
                    }
                    // Return the first projection result (caller can request multiple).
                    projections[0](&filtered_batch)
                });

            let elapsed = start.elapsed().as_micros() as u64;
            Ok(CompiledExpression {
                name: "filter_project_fused".to_string(),
                tier: CompilationTier::Fused,
                input_types: vec![],
                output_type,
                evaluator,
                compilation_time_us: elapsed,
                invocation_count: AtomicU64::new(0),
            })
        })
    }

    /// Fuse chained arithmetic expressions into a single evaluation pass.
    ///
    /// Each expression takes a `RecordBatch` and returns an `ArrayRef`. The chain
    /// feeds each result into the next expression via a temporary single-column batch.
    pub fn compile_arithmetic_chain(
        &self,
        exprs: Vec<Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>>,
        output_type: DataType,
    ) -> Result<Arc<CompiledExpression>> {
        if !self.config.enabled {
            return Err(BlazeError::execution("JIT compilation is disabled"));
        }
        if exprs.is_empty() {
            return Err(BlazeError::invalid_argument(
                "Arithmetic chain must have at least one expression",
            ));
        }

        let fingerprint = format!("arithmetic_chain_{}", exprs.len());

        self.cache.get_or_compile(&fingerprint, move || {
            let start = Instant::now();

            let evaluator: Box<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync> =
                Box::new(move |batch: &RecordBatch| {
                    let mut result = exprs[0](batch)?;
                    for expr in exprs.iter().skip(1) {
                        // Build a single-column batch from previous result for chaining.
                        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                            arrow::datatypes::Field::new(
                                "_chain",
                                result.data_type().clone(),
                                true,
                            ),
                        ]));
                        let intermediate = RecordBatch::try_new(schema, vec![result])?;
                        result = expr(&intermediate)?;
                    }
                    Ok(result)
                });

            let elapsed = start.elapsed().as_micros() as u64;
            Ok(CompiledExpression {
                name: "arithmetic_chain_fused".to_string(),
                tier: CompilationTier::Fused,
                input_types: vec![],
                output_type,
                evaluator,
                compilation_time_us: elapsed,
                invocation_count: AtomicU64::new(0),
            })
        })
    }

    /// Fuse AND predicate chains with short-circuit evaluation.
    ///
    /// Evaluates predicates in order. After each predicate, rows that are already
    /// `false` are skipped for subsequent predicates, avoiding unnecessary work.
    pub fn compile_predicate_chain(
        &self,
        predicates: Vec<Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>>,
        output_type: DataType,
    ) -> Result<Arc<CompiledExpression>> {
        if !self.config.enabled {
            return Err(BlazeError::execution("JIT compilation is disabled"));
        }
        if predicates.is_empty() {
            return Err(BlazeError::invalid_argument(
                "Predicate chain must have at least one predicate",
            ));
        }

        let fingerprint = format!("predicate_chain_{}", predicates.len());

        self.cache.get_or_compile(&fingerprint, move || {
            let start = Instant::now();

            let evaluator: Box<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync> =
                Box::new(move |batch: &RecordBatch| {
                    // Start with the first predicate as the cumulative mask.
                    let first = predicates[0](batch)?;
                    let mut combined = first
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| BlazeError::execution("Predicate must return BooleanArray"))?
                        .clone();

                    // Short-circuit: AND each subsequent predicate into the mask.
                    for pred in predicates.iter().skip(1) {
                        // If no rows remain true, short-circuit.
                        if combined.true_count() == 0 {
                            break;
                        }

                        let next = pred(batch)?;
                        let next_mask =
                            next.as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or_else(|| {
                                    BlazeError::execution("Predicate must return BooleanArray")
                                })?;

                        combined = arrow::compute::kernels::boolean::and(&combined, next_mask)?;
                    }

                    Ok(Arc::new(combined) as ArrayRef)
                });

            let elapsed = start.elapsed().as_micros() as u64;
            Ok(CompiledExpression {
                name: "predicate_chain_fused".to_string(),
                tier: CompilationTier::Fused,
                input_types: vec![],
                output_type,
                evaluator,
                compilation_time_us: elapsed,
                invocation_count: AtomicU64::new(0),
            })
        })
    }

    /// Get a reference to the compilation cache.
    pub fn cache(&self) -> &ExpressionCompilationCache {
        &self.cache
    }
}

// ---------------------------------------------------------------------------
// Expression IR — intermediate representation for vectorized compilation
// ---------------------------------------------------------------------------

/// Opcodes for the expression IR.
#[derive(Debug, Clone, PartialEq)]
pub enum IrOp {
    /// Load a column by index
    LoadColumn(usize),
    /// Load a literal value
    LoadLiteral(IrLiteral),
    /// Arithmetic: add top two stack values
    Add,
    /// Arithmetic: subtract
    Sub,
    /// Arithmetic: multiply
    Mul,
    /// Arithmetic: divide
    Div,
    /// Arithmetic: modulo
    Mod,
    /// Comparison: equal
    Eq,
    /// Comparison: not equal
    Ne,
    /// Comparison: less than
    Lt,
    /// Comparison: less than or equal
    Le,
    /// Comparison: greater than
    Gt,
    /// Comparison: greater than or equal
    Ge,
    /// Logical AND
    And,
    /// Logical OR
    Or,
    /// Logical NOT (unary)
    Not,
    /// Negate (unary)
    Neg,
    /// Cast top of stack to target type
    Cast(DataType),
    /// Apply a named scalar function
    Call { name: String, arg_count: usize },
}

/// Literal values in the IR.
#[derive(Debug, Clone, PartialEq)]
pub enum IrLiteral {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Null,
}

/// A compiled IR program: a sequence of stack-based opcodes.
#[derive(Debug, Clone)]
pub struct IrProgram {
    /// The opcode sequence
    pub ops: Vec<IrOp>,
    /// Output type of the expression
    pub output_type: DataType,
    /// Human-readable name
    pub name: String,
}

impl IrProgram {
    pub fn new(name: impl Into<String>, output_type: DataType) -> Self {
        Self {
            ops: Vec::new(),
            output_type,
            name: name.into(),
        }
    }

    pub fn push(&mut self, op: IrOp) {
        self.ops.push(op);
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Codegen Backend — abstraction for code generation targets
// ---------------------------------------------------------------------------

/// Code generation target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodegenTarget {
    /// Interpreted execution via closures (default, always available)
    Interpreted,
    /// Type-specialized fast paths for common patterns
    Specialized,
    /// Fused multi-expression pipeline
    Fused,
}

/// Result of code generation from an IR program.
pub struct GeneratedKernel {
    pub name: String,
    pub target: CodegenTarget,
    pub evaluator: Box<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>,
}

impl std::fmt::Debug for GeneratedKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratedKernel")
            .field("name", &self.name)
            .field("target", &self.target)
            .finish()
    }
}

/// Backend for generating executable kernels from IR programs.
pub struct CodegenBackend;

impl CodegenBackend {
    /// Generate a kernel from an IR program.
    ///
    /// For simple column arithmetic, produces type-specialized fast paths.
    /// For complex expressions, produces interpreted closures.
    pub fn generate(program: &IrProgram) -> Result<GeneratedKernel> {
        // Detect patterns eligible for specialized fast paths
        if let Some(kernel) = Self::try_specialize_column_binop(program) {
            return Ok(kernel);
        }

        // Default: stack-based interpreted evaluation
        Self::generate_interpreted(program)
    }

    /// Try to generate a specialized kernel for `column OP column` patterns.
    fn try_specialize_column_binop(program: &IrProgram) -> Option<GeneratedKernel> {
        if program.ops.len() != 3 {
            return None;
        }
        let (col_a, col_b, op) = match (&program.ops[0], &program.ops[1], &program.ops[2]) {
            (
                IrOp::LoadColumn(a),
                IrOp::LoadColumn(b),
                op @ (IrOp::Add | IrOp::Sub | IrOp::Mul),
            ) if program.output_type == DataType::Int64 => (*a, *b, op.clone()),
            _ => return None,
        };

        let name = program.name.clone();
        Some(GeneratedKernel {
            name: format!("{}_specialized", name),
            target: CodegenTarget::Specialized,
            evaluator: Box::new(move |batch: &RecordBatch| {
                let a = batch
                    .column(col_a)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64 column"))?;
                let b = batch
                    .column(col_b)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64 column"))?;

                let result: Int64Array = match op {
                    IrOp::Add => a
                        .iter()
                        .zip(b.iter())
                        .map(|(x, y)| match (x, y) {
                            (Some(x), Some(y)) => Some(x + y),
                            _ => None,
                        })
                        .collect(),
                    IrOp::Sub => a
                        .iter()
                        .zip(b.iter())
                        .map(|(x, y)| match (x, y) {
                            (Some(x), Some(y)) => Some(x - y),
                            _ => None,
                        })
                        .collect(),
                    IrOp::Mul => a
                        .iter()
                        .zip(b.iter())
                        .map(|(x, y)| match (x, y) {
                            (Some(x), Some(y)) => Some(x * y),
                            _ => None,
                        })
                        .collect(),
                    _ => unreachable!(),
                };
                Ok(Arc::new(result) as ArrayRef)
            }),
        })
    }

    /// Generate an interpreted (closure-based) kernel.
    fn generate_interpreted(program: &IrProgram) -> Result<GeneratedKernel> {
        let ops = program.ops.clone();
        let name = program.name.clone();

        Ok(GeneratedKernel {
            name: format!("{}_interpreted", name),
            target: CodegenTarget::Interpreted,
            evaluator: Box::new(move |batch: &RecordBatch| {
                let mut stack: Vec<ArrayRef> = Vec::new();
                for op in &ops {
                    match op {
                        IrOp::LoadColumn(idx) => {
                            stack.push(batch.column(*idx).clone());
                        }
                        IrOp::LoadLiteral(lit) => {
                            let arr = Self::literal_to_array(lit, batch.num_rows())?;
                            stack.push(arr);
                        }
                        IrOp::Add | IrOp::Sub | IrOp::Mul | IrOp::Div => {
                            let b = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let a = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let result = Self::eval_binop_static(op, &a, &b)?;
                            stack.push(result);
                        }
                        IrOp::Eq | IrOp::Ne | IrOp::Lt | IrOp::Le | IrOp::Gt | IrOp::Ge => {
                            let b = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let a = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let result = Self::eval_cmp_static(op, &a, &b)?;
                            stack.push(result);
                        }
                        IrOp::And | IrOp::Or => {
                            let b = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let a = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let result = Self::eval_logical_static(op, &a, &b)?;
                            stack.push(result);
                        }
                        IrOp::Not => {
                            let a = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let bool_arr = a
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or_else(|| BlazeError::type_error("NOT requires boolean"))?;
                            let result: BooleanArray =
                                bool_arr.iter().map(|v| v.map(|b| !b)).collect();
                            stack.push(Arc::new(result) as ArrayRef);
                        }
                        IrOp::Neg => {
                            let a = stack
                                .pop()
                                .ok_or_else(|| BlazeError::execution("Stack underflow"))?;
                            let int_arr = a
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| BlazeError::type_error("NEG requires int64"))?;
                            let result: Int64Array =
                                int_arr.iter().map(|v: Option<i64>| v.map(|x| -x)).collect();
                            stack.push(Arc::new(result) as ArrayRef);
                        }
                        _ => {
                            return Err(BlazeError::not_implemented(format!("IR op: {:?}", op)));
                        }
                    }
                }
                stack
                    .pop()
                    .ok_or_else(|| BlazeError::execution("Empty stack after evaluation"))
            }),
        })
    }

    fn literal_to_array(lit: &IrLiteral, num_rows: usize) -> Result<ArrayRef> {
        match lit {
            IrLiteral::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v; num_rows])) as ArrayRef),
            IrLiteral::Float64(v) => {
                use arrow::array::Float64Array;
                Ok(Arc::new(Float64Array::from(vec![*v; num_rows])) as ArrayRef)
            }
            IrLiteral::Boolean(v) => {
                Ok(Arc::new(BooleanArray::from(vec![*v; num_rows])) as ArrayRef)
            }
            IrLiteral::Utf8(v) => {
                use arrow::array::StringArray;
                let vals: Vec<&str> = (0..num_rows).map(|_| v.as_str()).collect();
                Ok(Arc::new(StringArray::from(vals)) as ArrayRef)
            }
            IrLiteral::Null => {
                Ok(Arc::new(Int64Array::from(vec![None::<i64>; num_rows])) as ArrayRef)
            }
        }
    }

    fn eval_binop_static(op: &IrOp, a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef> {
        let a_arr = a
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlazeError::type_error("Binop requires Int64"))?;
        let b_arr = b
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlazeError::type_error("Binop requires Int64"))?;
        let result: Int64Array = a_arr
            .iter()
            .zip(b_arr.iter())
            .map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => match op {
                    IrOp::Add => Some(x + y),
                    IrOp::Sub => Some(x - y),
                    IrOp::Mul => Some(x * y),
                    IrOp::Div => {
                        if y == 0 {
                            None
                        } else {
                            Some(x / y)
                        }
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect();
        Ok(Arc::new(result) as ArrayRef)
    }

    fn eval_cmp_static(op: &IrOp, a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef> {
        let a_arr = a
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlazeError::type_error("Comparison requires Int64"))?;
        let b_arr = b
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlazeError::type_error("Comparison requires Int64"))?;
        let result: BooleanArray = a_arr
            .iter()
            .zip(b_arr.iter())
            .map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => match op {
                    IrOp::Eq => Some(x == y),
                    IrOp::Ne => Some(x != y),
                    IrOp::Lt => Some(x < y),
                    IrOp::Le => Some(x <= y),
                    IrOp::Gt => Some(x > y),
                    IrOp::Ge => Some(x >= y),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        Ok(Arc::new(result) as ArrayRef)
    }

    fn eval_logical_static(op: &IrOp, a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef> {
        let a_arr = a
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| BlazeError::type_error("Logical op requires Boolean"))?;
        let b_arr = b
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| BlazeError::type_error("Logical op requires Boolean"))?;
        let result: BooleanArray = a_arr
            .iter()
            .zip(b_arr.iter())
            .map(|(x, y)| match (x, y, op) {
                (Some(x), Some(y), IrOp::And) => Some(x && y),
                (Some(x), Some(y), IrOp::Or) => Some(x || y),
                _ => None,
            })
            .collect();
        Ok(Arc::new(result) as ArrayRef)
    }
}

// ---------------------------------------------------------------------------
// Adaptive Compilation — profile-guided tiered compilation
// ---------------------------------------------------------------------------

/// Tracks execution statistics per expression for adaptive compilation.
pub struct AdaptiveCompiler {
    compiler: ExpressionCompiler,
    /// Maps fingerprint -> (invocation count, current tier)
    profiles: RwLock<HashMap<String, ExpressionProfile>>,
    /// Threshold for upgrading from Interpreted to Specialized/Fused
    upgrade_threshold: u64,
}

/// Profile of a single expression for adaptive tiering.
#[derive(Debug, Clone)]
pub struct ExpressionProfile {
    pub fingerprint: String,
    pub invocation_count: u64,
    pub total_eval_us: u64,
    pub current_tier: CompilationTier,
}

impl AdaptiveCompiler {
    /// Create a new adaptive compiler.
    pub fn new(config: CompilationConfig) -> Self {
        let upgrade_threshold = config.compilation_threshold;
        Self {
            compiler: ExpressionCompiler::new(config),
            profiles: RwLock::new(HashMap::new()),
            upgrade_threshold,
        }
    }

    /// Record an invocation and return the recommended tier.
    pub fn record_invocation(&self, fingerprint: &str, eval_time_us: u64) -> CompilationTier {
        let mut profiles = self.profiles.write();
        let profile =
            profiles
                .entry(fingerprint.to_string())
                .or_insert_with(|| ExpressionProfile {
                    fingerprint: fingerprint.to_string(),
                    invocation_count: 0,
                    total_eval_us: 0,
                    current_tier: CompilationTier::Interpreted,
                });
        profile.invocation_count += 1;
        profile.total_eval_us += eval_time_us;

        if profile.invocation_count >= self.upgrade_threshold
            && profile.current_tier == CompilationTier::Interpreted
        {
            profile.current_tier = CompilationTier::Specialized;
            CompilationTier::Specialized
        } else {
            profile.current_tier
        }
    }

    /// Compile an IR program with adaptive tiering.
    pub fn compile_adaptive(&self, program: &IrProgram) -> Result<GeneratedKernel> {
        let profiles = self.profiles.read();
        let tier = profiles
            .get(&program.name)
            .map(|p| p.current_tier)
            .unwrap_or(CompilationTier::Interpreted);
        drop(profiles);

        match tier {
            CompilationTier::Specialized | CompilationTier::Fused => {
                CodegenBackend::generate(program)
            }
            CompilationTier::Interpreted => CodegenBackend::generate_interpreted(program),
        }
    }

    /// Get the profile for an expression.
    pub fn get_profile(&self, fingerprint: &str) -> Option<ExpressionProfile> {
        self.profiles.read().get(fingerprint).cloned()
    }

    /// Get the underlying compiler.
    pub fn compiler(&self) -> &ExpressionCompiler {
        &self.compiler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// Helper: build a two-column Int64 batch (a, b).
    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", arrow::datatypes::DataType::Int64, false),
            Field::new("b", arrow::datatypes::DataType::Int64, false),
        ]));
        let a = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;
        let b = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        RecordBatch::try_new(schema, vec![a, b]).unwrap()
    }

    #[test]
    fn test_compilation_config_defaults() {
        let config = CompilationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.compilation_threshold, 100);
        assert_eq!(config.max_cache_entries, 1024);
        assert!(config.enable_fusion);
        assert!(config.enable_specialization);
    }

    #[test]
    fn test_compile_filter_project_fusion() {
        let compiler = ExpressionCompiler::new(CompilationConfig::default());
        let batch = make_test_batch();

        // Filter: a > 2
        let filter_expr: Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync> =
            Arc::new(|batch: &RecordBatch| {
                let a = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let mask: BooleanArray = a
                    .iter()
                    .map(|v: Option<i64>| v.map(|val| val > 2))
                    .collect();
                Ok(Arc::new(mask) as ArrayRef)
            });

        // Projection: column b (identity on filtered batch).
        let proj: Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync> =
            Arc::new(|batch: &RecordBatch| Ok(batch.column(1).clone()));

        let compiled = compiler
            .compile_filter_project(filter_expr, vec![proj], DataType::Int64)
            .unwrap();

        assert_eq!(compiled.tier, CompilationTier::Fused);
        assert_eq!(compiled.name, "filter_project_fused");

        let result = compiled.evaluate(&batch).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        // Rows where a > 2: a=[3,4,5], corresponding b=[30,40,50]
        assert_eq!(result_arr.len(), 3);
        assert_eq!(result_arr.value(0), 30);
        assert_eq!(result_arr.value(1), 40);
        assert_eq!(result_arr.value(2), 50);
    }

    #[test]
    fn test_compile_predicate_chain() {
        let compiler = ExpressionCompiler::new(CompilationConfig::default());
        let batch = make_test_batch();

        // Predicate 1: a > 1
        let p1: Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync> =
            Arc::new(|batch: &RecordBatch| {
                let a = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let mask: BooleanArray = a
                    .iter()
                    .map(|v: Option<i64>| v.map(|val| val > 1))
                    .collect();
                Ok(Arc::new(mask) as ArrayRef)
            });

        // Predicate 2: a < 5
        let p2: Arc<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync> =
            Arc::new(|batch: &RecordBatch| {
                let a = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let mask: BooleanArray = a
                    .iter()
                    .map(|v: Option<i64>| v.map(|val| val < 5))
                    .collect();
                Ok(Arc::new(mask) as ArrayRef)
            });

        let compiled = compiler
            .compile_predicate_chain(vec![p1, p2], DataType::Boolean)
            .unwrap();

        let result = compiled.evaluate(&batch).unwrap();
        let mask = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        // a > 1 AND a < 5 => [false, true, true, true, false]
        assert_eq!(mask.len(), 5);
        assert!(!mask.value(0));
        assert!(mask.value(1));
        assert!(mask.value(2));
        assert!(mask.value(3));
        assert!(!mask.value(4));
    }

    #[test]
    fn test_expression_cache_hit_miss() {
        let cache = ExpressionCompilationCache::new(CompilationConfig::default());

        // First call: cache miss.
        let _expr1 = cache
            .get_or_compile("key_a", || {
                Ok(CompiledExpression {
                    name: "a".to_string(),
                    tier: CompilationTier::Interpreted,
                    input_types: vec![],
                    output_type: DataType::Int64,
                    evaluator: Box::new(|_| Ok(Arc::new(Int64Array::from(vec![1])) as ArrayRef)),
                    compilation_time_us: 0,
                    invocation_count: AtomicU64::new(0),
                })
            })
            .unwrap();

        let stats = cache.stats();
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cache_hits, 0);

        // Second call with same key: cache hit.
        let _expr2 = cache
            .get_or_compile("key_a", || {
                panic!("should not compile again");
            })
            .unwrap();

        let stats = cache.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
    }

    #[test]
    fn test_compilation_stats_tracking() {
        let cache = ExpressionCompilationCache::new(CompilationConfig {
            max_cache_entries: 2,
            ..CompilationConfig::default()
        });

        for i in 0..3 {
            let _ = cache.get_or_compile(&format!("key_{i}"), || {
                Ok(CompiledExpression {
                    name: format!("expr_{i}"),
                    tier: CompilationTier::Fused,
                    input_types: vec![],
                    output_type: DataType::Int64,
                    evaluator: Box::new(|_| Ok(Arc::new(Int64Array::from(vec![1])) as ArrayRef)),
                    compilation_time_us: 10,
                    invocation_count: AtomicU64::new(0),
                })
            });
        }

        let stats = cache.stats();
        assert_eq!(stats.total_compilations, 3);
        assert_eq!(stats.cache_misses, 3);
        assert_eq!(stats.cache_hits, 0);
    }

    #[test]
    fn test_compiled_expression_invocation_count() {
        let batch = make_test_batch();

        let expr = CompiledExpression {
            name: "counter_test".to_string(),
            tier: CompilationTier::Interpreted,
            input_types: vec![],
            output_type: DataType::Int64,
            evaluator: Box::new(|batch: &RecordBatch| Ok(batch.column(0).clone())),
            compilation_time_us: 0,
            invocation_count: AtomicU64::new(0),
        };

        assert_eq!(expr.invocation_count(), 0);

        expr.evaluate(&batch).unwrap();
        expr.evaluate(&batch).unwrap();
        expr.evaluate(&batch).unwrap();

        assert_eq!(expr.invocation_count(), 3);
    }

    // --- IR and Codegen tests ---

    #[test]
    fn test_ir_program_build() {
        let mut prog = IrProgram::new("test_add", DataType::Int64);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadColumn(1));
        prog.push(IrOp::Add);
        assert_eq!(prog.len(), 3);
        assert!(!prog.is_empty());
        assert_eq!(prog.output_type, DataType::Int64);
    }

    #[test]
    fn test_codegen_specialized_add() {
        let batch = make_test_batch();
        let mut prog = IrProgram::new("col_add", DataType::Int64);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadColumn(1));
        prog.push(IrOp::Add);

        let kernel = CodegenBackend::generate(&prog).unwrap();
        assert_eq!(kernel.target, CodegenTarget::Specialized);

        let result = (kernel.evaluator)(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        // a=[1,2,3,4,5] + b=[10,20,30,40,50]
        assert_eq!(arr.value(0), 11);
        assert_eq!(arr.value(4), 55);
    }

    #[test]
    fn test_codegen_specialized_sub() {
        let batch = make_test_batch();
        let mut prog = IrProgram::new("col_sub", DataType::Int64);
        prog.push(IrOp::LoadColumn(1));
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::Sub);

        let kernel = CodegenBackend::generate(&prog).unwrap();
        assert_eq!(kernel.target, CodegenTarget::Specialized);

        let result = (kernel.evaluator)(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        // b=[10,20,30,40,50] - a=[1,2,3,4,5]
        assert_eq!(arr.value(0), 9);
        assert_eq!(arr.value(4), 45);
    }

    #[test]
    fn test_codegen_interpreted_complex() {
        let batch = make_test_batch();
        // (a + b) * a => interpreted because 5 ops, not simple pattern
        let mut prog = IrProgram::new("complex", DataType::Int64);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadColumn(1));
        prog.push(IrOp::Add);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::Mul);

        let kernel = CodegenBackend::generate(&prog).unwrap();
        assert_eq!(kernel.target, CodegenTarget::Interpreted);

        let result = (kernel.evaluator)(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        // row 0: (1+10)*1 = 11
        assert_eq!(arr.value(0), 11);
        // row 4: (5+50)*5 = 275
        assert_eq!(arr.value(4), 275);
    }

    #[test]
    fn test_codegen_comparison() {
        let batch = make_test_batch();
        let mut prog = IrProgram::new("cmp_gt", DataType::Boolean);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadLiteral(IrLiteral::Int64(3)));
        prog.push(IrOp::Gt);

        let kernel = CodegenBackend::generate(&prog).unwrap();
        let result = (kernel.evaluator)(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        // a=[1,2,3,4,5] > 3 => [false,false,false,true,true]
        assert!(!arr.value(0));
        assert!(!arr.value(2));
        assert!(arr.value(3));
        assert!(arr.value(4));
    }

    #[test]
    fn test_codegen_logical_and() {
        let batch = make_test_batch();
        // a > 1 AND a < 5
        let mut prog = IrProgram::new("logical_and", DataType::Boolean);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadLiteral(IrLiteral::Int64(1)));
        prog.push(IrOp::Gt);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadLiteral(IrLiteral::Int64(5)));
        prog.push(IrOp::Lt);
        prog.push(IrOp::And);

        let kernel = CodegenBackend::generate(&prog).unwrap();
        let result = (kernel.evaluator)(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        // a=[1,2,3,4,5]: > 1 AND < 5 => [false,true,true,true,false]
        assert!(!arr.value(0));
        assert!(arr.value(1));
        assert!(arr.value(3));
        assert!(!arr.value(4));
    }

    #[test]
    fn test_codegen_not() {
        let batch = make_test_batch();
        // NOT (a > 3)
        let mut prog = IrProgram::new("not_expr", DataType::Boolean);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadLiteral(IrLiteral::Int64(3)));
        prog.push(IrOp::Gt);
        prog.push(IrOp::Not);

        let kernel = CodegenBackend::generate(&prog).unwrap();
        let result = (kernel.evaluator)(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        // NOT (a > 3): [true,true,true,false,false]
        assert!(arr.value(0));
        assert!(arr.value(2));
        assert!(!arr.value(3));
    }

    #[test]
    fn test_adaptive_compiler_tiering() {
        let config = CompilationConfig {
            compilation_threshold: 3,
            ..CompilationConfig::default()
        };
        let adaptive = AdaptiveCompiler::new(config);

        // Initially interpreted
        let tier = adaptive.record_invocation("expr_1", 100);
        assert_eq!(tier, CompilationTier::Interpreted);

        let tier = adaptive.record_invocation("expr_1", 100);
        assert_eq!(tier, CompilationTier::Interpreted);

        // Third invocation triggers upgrade
        let tier = adaptive.record_invocation("expr_1", 100);
        assert_eq!(tier, CompilationTier::Specialized);

        let profile = adaptive.get_profile("expr_1").unwrap();
        assert_eq!(profile.invocation_count, 3);
        assert_eq!(profile.total_eval_us, 300);
        assert_eq!(profile.current_tier, CompilationTier::Specialized);
    }

    #[test]
    fn test_adaptive_compile_program() {
        let adaptive = AdaptiveCompiler::new(CompilationConfig::default());
        let mut prog = IrProgram::new("test_prog", DataType::Int64);
        prog.push(IrOp::LoadColumn(0));
        prog.push(IrOp::LoadColumn(1));
        prog.push(IrOp::Add);

        let kernel = adaptive.compile_adaptive(&prog).unwrap();
        // No invocations yet, should be interpreted tier
        assert_eq!(kernel.target, CodegenTarget::Interpreted);
    }
}
