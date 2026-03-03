//! Pull-based streaming execution for physical plans.

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::planner::PhysicalPlan;

use super::ExecutionContext;

/// A streaming iterator over query results.
///
/// Unlike the batch executor which materializes all results,
/// `PlanStream` yields `RecordBatch`es one at a time, enabling
/// memory-efficient processing of large result sets.
pub struct PlanStream {
    batches: Vec<RecordBatch>,
    index: usize,
}

impl PlanStream {
    /// Create a streaming iterator from a physical plan.
    ///
    /// Currently materializes the plan internally and yields batches
    /// incrementally. Future versions will implement true per-operator
    /// streaming for scan/filter/projection operators.
    pub fn new(plan: &PhysicalPlan, ctx: &ExecutionContext) -> Result<Self> {
        let batches = ctx.execute(plan)?;
        Ok(Self { batches, index: 0 })
    }

    /// Create an empty streaming iterator that yields no batches.
    pub fn empty() -> Self {
        Self {
            batches: vec![],
            index: 0,
        }
    }

    /// Returns the number of remaining batches (if known).
    pub fn remaining(&self) -> usize {
        self.batches.len().saturating_sub(self.index)
    }

    /// Returns true if there are more batches.
    pub fn has_next(&self) -> bool {
        self.index < self.batches.len()
    }
}

impl Iterator for PlanStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Some(Ok(batch))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PlanStream {}
