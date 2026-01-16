//! Query execution engine for Blaze.
//!
//! This module provides the vectorized execution engine that processes
//! physical plans and produces Arrow RecordBatches.

mod operators;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

pub use operators::*;

use crate::catalog::CatalogList;
use crate::error::Result;
use crate::planner::{ExecutionStats, JoinType, PhysicalPlan, TimeTravelSpec};

/// Execution context for query execution.
#[derive(Clone)]
pub struct ExecutionContext {
    /// Batch size for vectorized execution
    batch_size: usize,
    /// Optional catalog list for table lookups during execution
    catalog_list: Option<Arc<CatalogList>>,
    /// Memory manager for enforcing memory limits
    memory_manager: Arc<MemoryManager>,
}

impl ExecutionContext {
    /// Create a new execution context.
    pub fn new() -> Self {
        Self {
            batch_size: 8192,
            catalog_list: None,
            memory_manager: Arc::new(MemoryManager::default_budget()),
        }
    }

    /// Minimum allowed batch size.
    const MIN_BATCH_SIZE: usize = 1;
    /// Maximum allowed batch size (16 million rows).
    const MAX_BATCH_SIZE: usize = 16_777_216;

    /// Set the batch size.
    ///
    /// # Panics
    /// Panics if batch_size is 0 or exceeds MAX_BATCH_SIZE.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        assert!(
            (Self::MIN_BATCH_SIZE..=Self::MAX_BATCH_SIZE).contains(&batch_size),
            "batch_size must be between {} and {}, got {}",
            Self::MIN_BATCH_SIZE,
            Self::MAX_BATCH_SIZE,
            batch_size
        );
        self.batch_size = batch_size;
        self
    }

    /// Set the catalog list for table lookups.
    pub fn with_catalog_list(mut self, catalog_list: Arc<CatalogList>) -> Self {
        self.catalog_list = Some(catalog_list);
        self
    }

    /// Set the memory limit for query execution.
    pub fn with_memory_limit(mut self, limit: usize) -> Self {
        self.memory_manager = Arc::new(MemoryManager::new(limit));
        self
    }

    /// Get the batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get the memory manager.
    pub fn memory_manager(&self) -> &Arc<MemoryManager> {
        &self.memory_manager
    }

    /// Execute a physical plan and return the results.
    pub fn execute(&self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        let mut executor = Executor::new(
            self.batch_size,
            self.catalog_list.clone(),
            self.memory_manager.clone(),
        );
        executor.execute(plan)
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

/// The query executor.
pub struct Executor {
    /// Batch size for processing
    #[allow(dead_code)]
    batch_size: usize,
    /// Catalog list for table lookups
    catalog_list: Option<Arc<CatalogList>>,
    /// Memory manager for tracking and limiting memory usage
    memory_manager: Arc<MemoryManager>,
}

impl Executor {
    /// Create a new executor.
    pub fn new(
        batch_size: usize,
        catalog_list: Option<Arc<CatalogList>>,
        memory_manager: Arc<MemoryManager>,
    ) -> Self {
        Self {
            batch_size,
            catalog_list,
            memory_manager,
        }
    }

    /// Track memory usage for a batch and return error if over limit.
    fn track_batch_memory(&self, batch: &RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size();
        if !self.memory_manager.try_reserve(size) {
            return Err(crate::error::BlazeError::resource_exhausted(format!(
                "Memory limit exceeded while processing batch of {} bytes",
                size
            )));
        }
        Ok(())
    }

    /// Track memory usage for multiple batches.
    fn track_batches_memory(&self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.track_batch_memory(batch)?;
        }
        Ok(())
    }

    /// Execute a physical plan.
    pub fn execute(&mut self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        let result = self.execute_plan(plan)?;
        // Track memory usage for result batches
        self.track_batches_memory(&result)?;
        Ok(result)
    }

    /// Internal execute implementation without memory tracking (to avoid double-counting).
    fn execute_plan(&mut self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        match plan {
            PhysicalPlan::Scan {
                table_name,
                projection,
                schema,
                filters,
                time_travel,
            } => self.execute_scan(
                table_name,
                projection.as_deref(),
                schema,
                filters,
                time_travel.as_ref(),
            ),
            PhysicalPlan::Filter { predicate, input } => {
                let input_batches = self.execute_plan(input)?;
                self.execute_filter(predicate, input_batches)
            }
            PhysicalPlan::Projection {
                exprs,
                schema,
                input,
            } => {
                let input_batches = self.execute_plan(input)?;
                self.execute_projection(exprs, schema, input_batches)
            }
            PhysicalPlan::HashAggregate {
                group_by,
                aggr_exprs,
                schema,
                input,
            } => {
                let input_batches = self.execute_plan(input)?;
                self.execute_hash_aggregate(group_by, aggr_exprs, schema, input_batches)
            }
            PhysicalPlan::Sort { exprs, input } => {
                let input_batches = self.execute_plan(input)?;
                self.execute_sort(exprs, input_batches)
            }
            PhysicalPlan::Limit { skip, fetch, input } => {
                let input_batches = self.execute_plan(input)?;
                self.execute_limit(*skip, *fetch, input_batches)
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => {
                let left_batches = self.execute_plan(left)?;
                let right_batches = self.execute_plan(right)?;
                self.execute_hash_join(
                    left_batches,
                    right_batches,
                    *join_type,
                    left_keys,
                    right_keys,
                    schema,
                )
            }
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let left_batches = self.execute_plan(left)?;
                let right_batches = self.execute_plan(right)?;
                self.execute_cross_join(left_batches, right_batches, schema)
            }
            PhysicalPlan::Union { inputs, .. } => {
                let mut result = Vec::new();
                for input in inputs {
                    result.extend(self.execute_plan(input)?);
                }
                Ok(result)
            }
            PhysicalPlan::Values { data, .. } => Ok(data.clone()),
            PhysicalPlan::Empty {
                produce_one_row,
                schema,
            } => self.execute_empty(*produce_one_row, schema),
            PhysicalPlan::Explain {
                input,
                verbose,
                schema,
            } => self.execute_explain(input, *verbose, schema),
            PhysicalPlan::Window {
                window_exprs,
                schema,
                input,
            } => {
                let input_batches = self.execute_plan(input)?;
                self.execute_window(window_exprs, schema, input_batches)
            }
            PhysicalPlan::ExplainAnalyze {
                input,
                verbose,
                schema,
            } => self.execute_explain_analyze(input, *verbose, schema),
            PhysicalPlan::SortMergeJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => {
                let left_batches = self.execute_plan(left)?;
                let right_batches = self.execute_plan(right)?;
                operators::SortMergeJoinOperator::execute(
                    left_batches,
                    right_batches,
                    *join_type,
                    left_keys,
                    right_keys,
                    schema,
                )
            }
            PhysicalPlan::Copy {
                input,
                target,
                format,
                options,
                ..
            } => {
                let input_batches = self.execute_plan(input)?;
                self.execute_copy(&input_batches, target, format, options)
            }
        }
    }

    /// Execute a physical plan with statistics collection.
    pub fn execute_with_stats(
        &mut self,
        plan: &PhysicalPlan,
    ) -> Result<(Vec<RecordBatch>, ExecutionStats)> {
        use std::time::Instant;

        let start = Instant::now();
        let operator_name = self.get_operator_name(plan);
        let mut stats = ExecutionStats::new(&operator_name);

        let result = match plan {
            PhysicalPlan::Scan {
                table_name,
                projection,
                schema,
                filters,
                time_travel,
            } => {
                let batches = self.execute_scan(
                    table_name,
                    projection.as_deref(),
                    schema,
                    filters,
                    time_travel.as_ref(),
                )?;
                stats.add_metric("table", table_name);
                batches
            }
            PhysicalPlan::Filter { predicate, input } => {
                let (input_batches, child_stats) = self.execute_with_stats(input)?;
                stats.children.push(child_stats);
                stats.rows_processed = input_batches.iter().map(|b| b.num_rows()).sum();
                self.execute_filter(predicate, input_batches)?
            }
            PhysicalPlan::Projection {
                exprs,
                schema,
                input,
            } => {
                let (input_batches, child_stats) = self.execute_with_stats(input)?;
                stats.children.push(child_stats);
                stats.rows_processed = input_batches.iter().map(|b| b.num_rows()).sum();
                self.execute_projection(exprs, schema, input_batches)?
            }
            PhysicalPlan::HashAggregate {
                group_by,
                aggr_exprs,
                schema,
                input,
            } => {
                let (input_batches, child_stats) = self.execute_with_stats(input)?;
                stats.children.push(child_stats);
                stats.rows_processed = input_batches.iter().map(|b| b.num_rows()).sum();
                stats.add_metric("group_by_keys", &group_by.len().to_string());
                stats.add_metric("aggregates", &aggr_exprs.len().to_string());
                self.execute_hash_aggregate(group_by, aggr_exprs, schema, input_batches)?
            }
            PhysicalPlan::Sort { exprs, input } => {
                let (input_batches, child_stats) = self.execute_with_stats(input)?;
                stats.children.push(child_stats);
                stats.rows_processed = input_batches.iter().map(|b| b.num_rows()).sum();
                stats.add_metric("sort_keys", &exprs.len().to_string());
                self.execute_sort(exprs, input_batches)?
            }
            PhysicalPlan::Limit { skip, fetch, input } => {
                let (input_batches, child_stats) = self.execute_with_stats(input)?;
                stats.children.push(child_stats);
                stats.rows_processed = input_batches.iter().map(|b| b.num_rows()).sum();
                stats.add_metric("skip", &skip.to_string());
                stats.add_metric(
                    "fetch",
                    &fetch.map(|f| f.to_string()).unwrap_or("ALL".to_string()),
                );
                self.execute_limit(*skip, *fetch, input_batches)?
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => {
                let (left_batches, left_stats) = self.execute_with_stats(left)?;
                let (right_batches, right_stats) = self.execute_with_stats(right)?;
                stats.children.push(left_stats);
                stats.children.push(right_stats);
                let left_rows: usize = left_batches.iter().map(|b| b.num_rows()).sum();
                let right_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
                stats.rows_processed = left_rows + right_rows;
                stats.add_metric("join_type", &format!("{:?}", join_type));
                stats.add_metric("left_rows", &left_rows.to_string());
                stats.add_metric("right_rows", &right_rows.to_string());
                self.execute_hash_join(
                    left_batches,
                    right_batches,
                    *join_type,
                    left_keys,
                    right_keys,
                    schema,
                )?
            }
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let (left_batches, left_stats) = self.execute_with_stats(left)?;
                let (right_batches, right_stats) = self.execute_with_stats(right)?;
                stats.children.push(left_stats);
                stats.children.push(right_stats);
                stats.rows_processed = left_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                    + right_batches.iter().map(|b| b.num_rows()).sum::<usize>();
                self.execute_cross_join(left_batches, right_batches, schema)?
            }
            PhysicalPlan::Union { inputs, .. } => {
                let mut result = Vec::new();
                for input in inputs {
                    let (batches, child_stats) = self.execute_with_stats(input)?;
                    stats.children.push(child_stats);
                    result.extend(batches);
                }
                stats.rows_processed = result.iter().map(|b| b.num_rows()).sum();
                result
            }
            PhysicalPlan::Values { data, .. } => {
                stats.rows_processed = data.iter().map(|b| b.num_rows()).sum();
                data.clone()
            }
            PhysicalPlan::Empty {
                produce_one_row,
                schema,
            } => self.execute_empty(*produce_one_row, schema)?,
            PhysicalPlan::Window {
                window_exprs,
                schema,
                input,
            } => {
                let (input_batches, child_stats) = self.execute_with_stats(input)?;
                stats.children.push(child_stats);
                stats.rows_processed = input_batches.iter().map(|b| b.num_rows()).sum();
                stats.add_metric("window_functions", &window_exprs.len().to_string());
                self.execute_window(window_exprs, schema, input_batches)?
            }
            PhysicalPlan::Explain {
                input,
                verbose,
                schema,
            } => self.execute_explain(input, *verbose, schema)?,
            PhysicalPlan::ExplainAnalyze {
                input,
                verbose,
                schema,
            } => self.execute_explain_analyze(input, *verbose, schema)?,
            PhysicalPlan::SortMergeJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => {
                let (left_batches, left_stats) = self.execute_with_stats(left)?;
                let (right_batches, right_stats) = self.execute_with_stats(right)?;
                stats.children.push(left_stats);
                stats.children.push(right_stats);
                let left_rows: usize = left_batches.iter().map(|b| b.num_rows()).sum();
                let right_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
                stats.rows_processed = left_rows + right_rows;
                stats.add_metric("join_type", &format!("{:?}", join_type));
                stats.add_metric("left_rows", &left_rows.to_string());
                stats.add_metric("right_rows", &right_rows.to_string());
                operators::SortMergeJoinOperator::execute(
                    left_batches,
                    right_batches,
                    *join_type,
                    left_keys,
                    right_keys,
                    schema,
                )?
            }
            PhysicalPlan::Copy {
                input,
                target,
                format,
                options,
                ..
            } => {
                let (input_batches, child_stats) = self.execute_with_stats(input)?;
                stats.children.push(child_stats);
                stats.rows_processed = input_batches.iter().map(|b| b.num_rows()).sum();
                stats.add_metric("target", target);
                stats.add_metric("format", &format!("{:?}", format));
                self.execute_copy(&input_batches, target, format, options)?
            }
        };

        let elapsed = start.elapsed();
        stats.elapsed_nanos = elapsed.as_nanos() as u64;
        stats.batches_processed = result.len();
        stats.rows_output = result.iter().map(|b| b.num_rows()).sum();

        // Estimate memory usage (rough approximation based on batch sizes)
        stats.peak_memory_bytes = result.iter().map(|b| b.get_array_memory_size()).sum();

        Ok((result, stats))
    }

    fn get_operator_name(&self, plan: &PhysicalPlan) -> String {
        match plan {
            PhysicalPlan::Scan { table_name, .. } => format!("Scan({})", table_name),
            PhysicalPlan::Filter { .. } => "Filter".to_string(),
            PhysicalPlan::Projection { .. } => "Projection".to_string(),
            PhysicalPlan::HashAggregate { .. } => "HashAggregate".to_string(),
            PhysicalPlan::Sort { .. } => "Sort".to_string(),
            PhysicalPlan::Limit { .. } => "Limit".to_string(),
            PhysicalPlan::HashJoin { join_type, .. } => format!("HashJoin({:?})", join_type),
            PhysicalPlan::CrossJoin { .. } => "CrossJoin".to_string(),
            PhysicalPlan::SortMergeJoin { join_type, .. } => {
                format!("SortMergeJoin({:?})", join_type)
            }
            PhysicalPlan::Union { .. } => "Union".to_string(),
            PhysicalPlan::Values { .. } => "Values".to_string(),
            PhysicalPlan::Empty { .. } => "Empty".to_string(),
            PhysicalPlan::Explain { .. } => "Explain".to_string(),
            PhysicalPlan::Window { .. } => "Window".to_string(),
            PhysicalPlan::ExplainAnalyze { .. } => "ExplainAnalyze".to_string(),
            PhysicalPlan::Copy { target, .. } => format!("Copy({})", target),
        }
    }

    fn execute_scan(
        &self,
        table_name: &str,
        projection: Option<&[usize]>,
        schema: &Arc<arrow::datatypes::Schema>,
        filters: &[Arc<dyn crate::planner::PhysicalExpr>],
        time_travel: Option<&TimeTravelSpec>,
    ) -> Result<Vec<RecordBatch>> {
        // Try to get the table from catalog
        if let Some(catalog_list) = &self.catalog_list {
            if let Some(catalog) = catalog_list.catalog("default") {
                if let Some(table) = catalog.get_table(table_name) {
                    // TODO: For time travel, we need to get the Delta table at a specific version
                    // For now, time_travel is used as metadata but actual implementation
                    // will be enhanced when we add Delta-specific catalog support.
                    let _ = time_travel; // Suppress unused warning for now

                    // Use scan_with_filters for filter pushdown when supported
                    let batches = if table.supports_filter_pushdown() && !filters.is_empty() {
                        table.scan_with_filters(projection, filters, None)?
                    } else {
                        table.scan(projection, &[], None)?
                    };

                    // If we got data, return it
                    if !batches.is_empty() {
                        return Ok(batches);
                    }
                }
            }
        }

        // If no catalog or table not found, return empty batch with correct schema
        Ok(vec![RecordBatch::new_empty(schema.clone())])
    }

    fn execute_filter(
        &self,
        predicate: &Arc<dyn crate::planner::PhysicalExpr>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::BooleanArray;
        use arrow::compute::filter_record_batch;

        let mut result = Vec::new();

        for batch in input_batches {
            // Evaluate predicate
            let filter_array = predicate.evaluate(&batch)?;
            let filter_array = filter_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    crate::error::BlazeError::type_error("Filter must return boolean")
                })?;

            // Apply filter
            let filtered = filter_record_batch(&batch, filter_array)?;
            if filtered.num_rows() > 0 {
                result.push(filtered);
            }
        }

        Ok(result)
    }

    fn execute_projection(
        &self,
        exprs: &[Arc<dyn crate::planner::PhysicalExpr>],
        schema: &Arc<arrow::datatypes::Schema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();

        for batch in input_batches {
            let columns: Result<Vec<_>> = exprs.iter().map(|expr| expr.evaluate(&batch)).collect();

            let projected = RecordBatch::try_new(schema.clone(), columns?)?;
            result.push(projected);
        }

        Ok(result)
    }

    fn execute_hash_aggregate(
        &self,
        group_by: &[Arc<dyn crate::planner::PhysicalExpr>],
        aggr_exprs: &[crate::planner::AggregateExpr],
        schema: &Arc<arrow::datatypes::Schema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        HashAggregateOperator::execute(group_by, aggr_exprs, schema, input_batches)
    }

    fn execute_sort(
        &self,
        exprs: &[crate::planner::SortExpr],
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        SortOperator::execute(exprs, input_batches)
    }

    fn execute_limit(
        &self,
        skip: usize,
        fetch: Option<usize>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let mut skipped = 0;
        let mut taken = 0;
        let limit = fetch.unwrap_or(usize::MAX);

        for batch in input_batches {
            let batch_rows = batch.num_rows();

            // Handle skip
            let skip_in_batch = (skip - skipped).min(batch_rows);
            skipped += skip_in_batch;

            if skip_in_batch >= batch_rows {
                continue;
            }

            // Handle fetch
            let remaining_rows = batch_rows - skip_in_batch;
            let take_rows = (limit - taken).min(remaining_rows);

            if take_rows > 0 {
                let sliced = batch.slice(skip_in_batch, take_rows);
                result.push(sliced);
                taken += take_rows;

                if taken >= limit {
                    break;
                }
            }
        }

        Ok(result)
    }

    fn execute_hash_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        left_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        right_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<Vec<RecordBatch>> {
        HashJoinOperator::execute(
            left_batches,
            right_batches,
            join_type,
            left_keys,
            right_keys,
            schema,
        )
    }

    fn execute_cross_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<Vec<RecordBatch>> {
        CrossJoinOperator::execute(left_batches, right_batches, schema)
    }

    fn execute_empty(
        &self,
        produce_one_row: bool,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<Vec<RecordBatch>> {
        if produce_one_row {
            // Produce one row - for empty schema we use try_new_with_options
            // which allows creating a batch with 0 columns but a row count
            let options =
                arrow::record_batch::RecordBatchOptions::default().with_row_count(Some(1));
            let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options)?;
            Ok(vec![batch])
        } else {
            Ok(vec![])
        }
    }

    fn execute_explain(
        &self,
        input: &PhysicalPlan,
        verbose: bool,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::StringArray;

        let plan_str = if verbose {
            format!("{:#?}", input)
        } else {
            input.display_indent(0)
        };

        let lines: Vec<Option<&str>> = plan_str.lines().map(Some).collect();
        let array = StringArray::from(lines);

        Ok(vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(array)],
        )?])
    }

    fn execute_explain_analyze(
        &mut self,
        input: &PhysicalPlan,
        verbose: bool,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::StringArray;
        use std::time::Instant;

        let start = Instant::now();

        // Execute the plan and collect statistics
        let (_results, stats) = self.execute_with_stats(input)?;

        let total_elapsed = start.elapsed();

        // Format the output
        let mut output = String::new();

        // Header
        output.push_str("EXPLAIN ANALYZE\n");
        output.push_str("===============\n\n");

        // Plan structure
        output.push_str("Query Plan:\n");
        output.push_str(&input.display_indent(0));
        output.push('\n');

        // Execution statistics
        output.push_str("Execution Statistics:\n");
        output.push_str("---------------------\n");
        output.push_str(&stats.format_tree(0));
        output.push('\n');

        // Summary
        let total_ms = total_elapsed.as_secs_f64() * 1000.0;
        output.push_str(&format!("Total Execution Time: {:.3}ms\n", total_ms));
        output.push_str(&format!("Total Rows Output: {}\n", stats.rows_output));

        if verbose {
            output.push_str("\nVerbose Plan:\n");
            output.push_str(&format!("{:#?}", input));
        }

        let lines: Vec<Option<&str>> = output.lines().map(Some).collect();
        let array = StringArray::from(lines);

        Ok(vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(array)],
        )?])
    }

    fn execute_window(
        &self,
        window_exprs: &[crate::planner::PhysicalWindowExpr],
        schema: &Arc<arrow::datatypes::Schema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        WindowOperator::execute(window_exprs, schema, input_batches)
    }

    fn execute_copy(
        &self,
        batches: &[RecordBatch],
        target: &str,
        format: &crate::planner::CopyFormat,
        options: &crate::planner::CopyOptions,
    ) -> Result<Vec<RecordBatch>> {
        use std::fs::File;

        if batches.is_empty() {
            return Ok(vec![]);
        }

        let schema = batches[0].schema();
        let file = File::create(target)?;

        match format {
            crate::planner::CopyFormat::Parquet => {
                use parquet::arrow::ArrowWriter;
                use parquet::basic::Compression;
                use parquet::file::properties::WriterProperties;

                let compression = match options.compression.as_deref() {
                    Some("zstd") | Some("ZSTD") => Compression::ZSTD(Default::default()),
                    Some("gzip") | Some("GZIP") => Compression::GZIP(Default::default()),
                    Some("lz4") | Some("LZ4") => Compression::LZ4,
                    Some("none") | Some("NONE") | Some("uncompressed") => Compression::UNCOMPRESSED,
                    _ => Compression::SNAPPY,
                };
                let props = WriterProperties::builder()
                    .set_compression(compression)
                    .build();
                let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.close()?;
            }
            crate::planner::CopyFormat::Csv => {
                use arrow::csv::WriterBuilder;
                let header = options.header;
                let delimiter = options.delimiter.unwrap_or(b',');
                let mut writer = WriterBuilder::new()
                    .with_header(header)
                    .with_delimiter(delimiter)
                    .build(file);
                for batch in batches {
                    writer.write(batch)?;
                }
            }
            crate::planner::CopyFormat::Json => {
                use arrow::json::LineDelimitedWriter;
                let mut writer = LineDelimitedWriter::new(file);
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
        }

        // Return empty result - COPY TO doesn't return data
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    #[test]
    fn test_execution_context_default() {
        let ctx = ExecutionContext::new();
        assert_eq!(ctx.batch_size(), 8192);
    }

    #[test]
    fn test_execution_context_with_batch_size() {
        let ctx = ExecutionContext::new().with_batch_size(1024);
        assert_eq!(ctx.batch_size(), 1024);
    }

    #[test]
    fn test_execution_context_with_memory_limit() {
        let ctx = ExecutionContext::new().with_memory_limit(1024 * 1024);
        // Memory manager should be set with the limit
        assert!(ctx.memory_manager().try_reserve(1000));
    }

    #[test]
    fn test_execution_context_default_impl() {
        let ctx = ExecutionContext::default();
        assert_eq!(ctx.batch_size(), 8192);
    }

    #[test]
    #[should_panic(expected = "batch_size must be between")]
    fn test_execution_context_invalid_batch_size_zero() {
        ExecutionContext::new().with_batch_size(0);
    }

    #[test]
    #[should_panic(expected = "batch_size must be between")]
    fn test_execution_context_invalid_batch_size_too_large() {
        ExecutionContext::new().with_batch_size(17_000_000);
    }

    #[test]
    fn test_executor_new() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);
        // Just verify construction works
        let _ = executor;
    }

    #[test]
    fn test_execute_limit_no_skip() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let result = executor.execute_limit(0, Some(3), vec![batch]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_execute_limit_with_skip() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let result = executor.execute_limit(2, Some(2), vec![batch]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_execute_limit_no_fetch() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        // No fetch means take all
        let result = executor.execute_limit(2, None, vec![batch]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // 5 - 2 skipped
    }

    #[test]
    fn test_execute_limit_across_batches() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
        )
        .unwrap();

        // Skip 2, take 3 across two batches
        let result = executor
            .execute_limit(2, Some(3), vec![batch1, batch2])
            .unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_execute_empty_no_row() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let result = executor.execute_empty(false, &schema).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_execute_empty_with_row() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        // Empty schema produces one row when produce_one_row is true
        let schema = Arc::new(ArrowSchema::empty());

        let result = executor.execute_empty(true, &schema).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_execute_explain() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let scan_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let explain_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "plan",
            DataType::Utf8,
            false,
        )]));

        let plan = PhysicalPlan::Scan {
            table_name: "test".to_string(),
            projection: None,
            schema: scan_schema,
            filters: vec![],
            time_travel: None,
        };

        let result = executor
            .execute_explain(&plan, false, &explain_schema)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].num_rows() > 0);
    }

    #[test]
    fn test_execute_values() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let mut executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let plan = PhysicalPlan::Values {
            data: vec![batch],
            schema,
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[test]
    fn test_execute_union() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let mut executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();

        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3, 4]))])
                .unwrap();

        let plan = PhysicalPlan::Union {
            inputs: vec![
                PhysicalPlan::Values {
                    data: vec![batch1],
                    schema: schema.clone(),
                },
                PhysicalPlan::Values {
                    data: vec![batch2],
                    schema: schema.clone(),
                },
            ],
            schema,
        };

        let result = executor.execute(&plan).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_get_operator_name() {
        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let scan_plan = PhysicalPlan::Scan {
            table_name: "users".to_string(),
            projection: None,
            schema: schema.clone(),
            filters: vec![],
            time_travel: None,
        };

        let name = executor.get_operator_name(&scan_plan);
        assert_eq!(name, "Scan(users)");

        let empty_plan = PhysicalPlan::Empty {
            produce_one_row: false,
            schema,
        };

        let name = executor.get_operator_name(&empty_plan);
        assert_eq!(name, "Empty");
    }

    #[test]
    fn test_memory_tracking() {
        // Create a small memory budget
        let memory_manager = Arc::new(MemoryManager::new(1024));
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        // Should succeed for small batch
        assert!(executor.track_batch_memory(&batch).is_ok());
    }

    #[test]
    fn test_sort_merge_join() {
        use crate::planner::{ColumnExpr, PhysicalExpr};

        let memory_manager = Arc::new(MemoryManager::default_budget());
        let mut executor = Executor::new(8192, None, memory_manager);

        // Left table schema: id, name
        let left_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Right table schema: id, value
        let right_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Output schema: all columns from both
        let output_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Left data: (1, "a"), (2, "b"), (3, "c")
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Right data: (2, 20), (3, 30), (4, 40)
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 3, 4])),
                Arc::new(Int64Array::from(vec![20, 30, 40])),
            ],
        )
        .unwrap();

        let left_key: Arc<dyn PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));

        let plan = PhysicalPlan::SortMergeJoin {
            left: Box::new(PhysicalPlan::Values {
                schema: left_schema,
                data: vec![left_batch],
            }),
            right: Box::new(PhysicalPlan::Values {
                schema: right_schema,
                data: vec![right_batch],
            }),
            join_type: JoinType::Inner,
            left_keys: vec![left_key],
            right_keys: vec![right_key],
            schema: output_schema,
        };

        let result = executor.execute(&plan).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        // Inner join should match rows with id 2 and 3
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_execute_filter() {
        use crate::planner::{BinaryExpr, ColumnExpr, LiteralExpr, PhysicalExpr};

        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();

        // Create filter predicate: id > 2
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("id", 0)),
            "gt",
            Arc::new(LiteralExpr::new(crate::types::ScalarValue::Int64(Some(2)))),
        ));

        let result = executor.execute_filter(&predicate, vec![batch]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // rows with id 3, 4, 5
    }

    #[test]
    fn test_execute_projection() {
        use crate::planner::{ColumnExpr, PhysicalExpr};

        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let input_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let output_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();

        // Project columns 0 and 2 (id and value)
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(ColumnExpr::new("id", 0)),
            Arc::new(ColumnExpr::new("value", 2)),
        ];

        let result = executor
            .execute_projection(&exprs, &output_schema, vec![batch])
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_columns(), 2);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_execute_hash_join() {
        use crate::planner::{ColumnExpr, PhysicalExpr};

        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let left_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let right_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("order", DataType::Utf8, false),
        ]));

        let join_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("user_id", DataType::Int64, false),
            Field::new("order", DataType::Utf8, false),
        ]));

        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            ],
        )
        .unwrap();

        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 2])),
                Arc::new(StringArray::from(vec!["Order1", "Order2", "Order3"])),
            ],
        )
        .unwrap();

        let left_keys: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(ColumnExpr::new("id", 0))];
        let right_keys: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(ColumnExpr::new("user_id", 0))];

        let result = executor
            .execute_hash_join(
                vec![left_batch],
                vec![right_batch],
                JoinType::Inner,
                &left_keys,
                &right_keys,
                &join_schema,
            )
            .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // id=1 matches once, id=2 matches twice
    }

    #[test]
    fn test_execute_sort() {
        use crate::planner::{ColumnExpr, PhysicalExpr, SortExpr};

        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![3, 1, 4, 1, 5, 9, 2, 6]))],
        )
        .unwrap();

        let sort_exprs = vec![SortExpr {
            expr: Arc::new(ColumnExpr::new("id", 0)) as Arc<dyn PhysicalExpr>,
            ascending: true,
            nulls_first: true,
        }];

        let result = executor.execute_sort(&sort_exprs, vec![batch]).unwrap();
        assert_eq!(result.len(), 1);

        let sorted_col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values: Vec<i64> = (0..sorted_col.len()).map(|i| sorted_col.value(i)).collect();
        assert_eq!(values, vec![1, 1, 2, 3, 4, 5, 6, 9]);
    }

    #[test]
    fn test_execute_hash_aggregate() {
        use crate::planner::{AggregateExpr, AggregateFunc, ColumnExpr, PhysicalExpr};

        let memory_manager = Arc::new(MemoryManager::default_budget());
        let executor = Executor::new(8192, None, memory_manager);

        let input_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let output_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let group_by: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(ColumnExpr::new("category", 0))];
        let aggr_exprs = vec![AggregateExpr {
            func: AggregateFunc::Count,
            args: vec![Arc::new(ColumnExpr::new("value", 1)) as Arc<dyn PhysicalExpr>],
            distinct: false,
            alias: None,
        }];

        let result = executor
            .execute_hash_aggregate(&group_by, &aggr_exprs, &output_schema, vec![batch])
            .unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // Two categories: A and B
    }
}
