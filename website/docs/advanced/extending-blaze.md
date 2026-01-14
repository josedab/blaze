---
sidebar_position: 3
---

# Extending Blaze

Blaze provides multiple extension points for customization. This guide covers how to extend various components.

## Extension Points

| Extension | Purpose | Complexity |
|-----------|---------|------------|
| [Custom Storage](#custom-storage-providers) | New data sources | Medium |
| [Custom Functions](#custom-scalar-functions) | New SQL functions | Low |
| [Custom Expressions](#custom-physical-expressions) | New expression types | Medium |
| [Custom Operators](#custom-physical-operators) | New execution operators | High |
| [Custom Optimizer Rules](#custom-optimizer-rules) | New optimization logic | High |

## Custom Storage Providers

See [Custom Storage Providers](/docs/advanced/custom-storage-providers) for detailed guide.

Quick example:

```rust
impl TableProvider for MyProvider {
    fn schema(&self) -> &Schema { &self.schema }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        // Implement data reading
    }

    fn as_any(&self) -> &dyn Any { self }
}

// Register
conn.register_table("my_table", Arc::new(MyProvider::new()))?;
```

## Custom Scalar Functions

Add new SQL functions:

```rust
use blaze::planner::{PhysicalExpr, ScalarFunctionExpr};
use arrow::array::{ArrayRef, Float64Array};
use arrow::record_batch::RecordBatch;

// Define function implementation
fn my_square(args: &[ArrayRef]) -> Result<ArrayRef> {
    let input = args[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| BlazeError::type_error("Float64", "other"))?;

    let result: Float64Array = input
        .iter()
        .map(|v| v.map(|x| x * x))
        .collect();

    Ok(Arc::new(result))
}

// Create expression
struct SquareExpr {
    arg: Arc<dyn PhysicalExpr>,
}

impl PhysicalExpr for SquareExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let input = self.arg.evaluate(batch)?;
        my_square(&[input])
    }

    fn data_type(&self) -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Float64
    }

    fn name(&self) -> &str {
        "square"
    }
}
```

### Function Registration

To make functions available in SQL, modify the binder:

```rust
// In planner/binder.rs, add to scalar function handling
match function_name.to_uppercase().as_str() {
    "SQUARE" => {
        // Create SquareExpr with bound argument
    }
    // ... existing functions
}
```

## Custom Physical Expressions

Create new expression types:

```rust
use blaze::planner::PhysicalExpr;
use blaze::Result;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

/// Expression that checks if a string matches a regex
#[derive(Debug)]
pub struct RegexMatchExpr {
    input: Arc<dyn PhysicalExpr>,
    pattern: regex::Regex,
}

impl RegexMatchExpr {
    pub fn new(input: Arc<dyn PhysicalExpr>, pattern: &str) -> Result<Self> {
        let pattern = regex::Regex::new(pattern)
            .map_err(|e| BlazeError::invalid_argument(format!("Invalid regex: {}", e)))?;

        Ok(Self { input, pattern })
    }
}

impl PhysicalExpr for RegexMatchExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        use arrow::array::{BooleanArray, StringArray};

        let input_array = self.input.evaluate(batch)?;
        let string_array = input_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BlazeError::type_error("String", "other"))?;

        let result: BooleanArray = string_array
            .iter()
            .map(|v| v.map(|s| self.pattern.is_match(s)))
            .collect();

        Ok(Arc::new(result))
    }

    fn data_type(&self) -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Boolean
    }

    fn name(&self) -> &str {
        "regex_match"
    }
}
```

## Custom Physical Operators

Add new execution operators:

```rust
use blaze::planner::PhysicalPlan;
use blaze::executor::Executor;
use blaze::Result;
use arrow::record_batch::RecordBatch;

/// Operator that samples random rows
pub struct SampleOperator {
    input: Box<PhysicalPlan>,
    sample_rate: f64,
    schema: Arc<Schema>,
}

impl SampleOperator {
    pub fn execute(&self, executor: &Executor) -> Result<Vec<RecordBatch>> {
        use rand::Rng;

        let input_batches = executor.execute(&self.input)?;
        let mut rng = rand::thread_rng();

        let mut result = Vec::new();
        for batch in input_batches {
            // Create boolean mask for sampling
            let mask: Vec<bool> = (0..batch.num_rows())
                .map(|_| rng.gen::<f64>() < self.sample_rate)
                .collect();

            let mask_array = arrow::array::BooleanArray::from(mask);
            let sampled = arrow::compute::filter_record_batch(&batch, &mask_array)?;

            if sampled.num_rows() > 0 {
                result.push(sampled);
            }
        }

        Ok(result)
    }
}
```

### Integrating Custom Operators

To integrate into the execution pipeline:

1. Add variant to `PhysicalPlan` enum
2. Handle in `Executor::execute()`
3. Create in `PhysicalPlanner::create_physical_plan()`

## Custom Optimizer Rules

Add optimization rules:

```rust
use blaze::planner::{LogicalPlan, OptimizerRule};
use blaze::Result;

/// Rule that eliminates redundant projects
pub struct EliminateRedundantProject;

impl OptimizerRule for EliminateRedundantProject {
    fn name(&self) -> &str {
        "eliminate_redundant_project"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { exprs, input } => {
                // Check if projection is identity (same columns, same order)
                if self.is_identity_projection(exprs, input.schema()) {
                    // Skip this projection
                    self.apply(input)
                } else {
                    Ok(LogicalPlan::Projection {
                        exprs: exprs.clone(),
                        input: Box::new(self.apply(input)?),
                    })
                }
            }
            // Recursively apply to children
            _ => self.apply_to_children(plan),
        }
    }
}

impl EliminateRedundantProject {
    fn is_identity_projection(&self, exprs: &[LogicalExpr], schema: &Schema) -> bool {
        if exprs.len() != schema.fields().len() {
            return false;
        }

        exprs.iter().enumerate().all(|(i, expr)| {
            matches!(expr, LogicalExpr::Column { index, .. } if *index == i)
        })
    }
}
```

### Registering Custom Rules

```rust
use blaze::planner::Optimizer;

let mut optimizer = Optimizer::default();
optimizer.add_rule(Box::new(EliminateRedundantProject));
```

## Custom Aggregate Functions

Implement custom aggregates:

```rust
use blaze::executor::Accumulator;

/// Computes the geometric mean
pub struct GeometricMeanAccumulator {
    product: f64,
    count: usize,
}

impl GeometricMeanAccumulator {
    pub fn new() -> Self {
        Self { product: 1.0, count: 0 }
    }
}

impl Accumulator for GeometricMeanAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let array = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| BlazeError::type_error("Float64", "other"))?;

        for value in array.iter().flatten() {
            self.product *= value;
            self.count += 1;
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| BlazeError::internal("Type mismatch in merge"))?;

        self.product *= other.product;
        self.count += other.count;

        Ok(())
    }

    fn finalize(&self) -> Result<ScalarValue> {
        if self.count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            let result = self.product.powf(1.0 / self.count as f64);
            Ok(ScalarValue::Float64(Some(result)))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

## Custom Window Functions

Implement custom window functions:

```rust
use blaze::executor::WindowFunction;

/// Custom window function: exponential moving average
pub struct ExponentialMovingAverage {
    alpha: f64,
}

impl WindowFunction for ExponentialMovingAverage {
    fn compute(
        &self,
        values: &ArrayRef,
        partition_ranges: &[(usize, usize)],
    ) -> Result<ArrayRef> {
        let input = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| BlazeError::type_error("Float64", "other"))?;

        let mut result = Vec::with_capacity(input.len());

        for (start, end) in partition_ranges {
            let mut ema = None;

            for i in *start..*end {
                let value = input.value(i);
                ema = Some(match ema {
                    None => value,
                    Some(prev) => self.alpha * value + (1.0 - self.alpha) * prev,
                });
                result.push(ema);
            }
        }

        Ok(Arc::new(Float64Array::from(
            result.into_iter().map(|v| v).collect::<Vec<_>>()
        )))
    }
}
```

## Best Practices

### 1. Follow Existing Patterns

Study existing implementations before creating new ones:

```rust
// Look at existing expressions in planner/physical_expr.rs
// Look at existing operators in executor/operators.rs
```

### 2. Handle NULL Values

Always handle NULL properly:

```rust
fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
    let input = self.input.evaluate(batch)?;

    // Handle NULLs
    let result: Float64Array = input
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|v| v.map(|x| self.transform(x)))  // Preserves NULL
        .collect();

    Ok(Arc::new(result))
}
```

### 3. Provide Good Error Messages

```rust
fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
    let input = self.input.evaluate(batch)?;

    let array = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| BlazeError::type_error(
            "String",
            &format!("{:?}", input.data_type())
        ))?;

    // ...
}
```

### 4. Write Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custom_function() {
        let input = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let result = my_square(&[input.clone()]).unwrap();

        let expected = Float64Array::from(vec![1.0, 4.0, 9.0]);
        assert_eq!(result.as_ref(), &expected);
    }
}
```

## Next Steps

- [Custom Storage Providers](/docs/advanced/custom-storage-providers) - Data source extensions
- [Query Optimization](/docs/advanced/query-optimization) - Optimizer internals
- [Architecture](/docs/concepts/architecture) - System design
