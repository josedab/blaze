# ADR-0008: Rule-Based Query Optimizer with Cost-Based Foundation

## Status

Accepted

## Context

Query optimization transforms logical plans into more efficient equivalent plans. Two main approaches exist:

**Rule-Based Optimization (RBO)**:
- Apply transformation rules in fixed order
- Deterministic, predictable results
- Easy to understand and debug
- May miss globally optimal plans
- Used by: Early Oracle, simple databases

**Cost-Based Optimization (CBO)**:
- Enumerate alternative plans
- Estimate cost using statistics
- Choose lowest-cost plan
- Can find globally optimal plans
- Complex implementation, statistics dependency
- Used by: Modern Oracle, PostgreSQL, Spark

**Hybrid Approach**:
- Rule-based for heuristic transformations (always beneficial)
- Cost-based for alternative selection (needs statistics)
- Practical balance of complexity and optimality
- Used by: Presto/Trino, DataFusion

## Decision

We implemented a **rule-based optimizer** with infrastructure for future cost-based optimization:

### Current: Rule-Based Optimizer

```rust
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

pub trait OptimizerRule: Debug {
    fn name(&self) -> &str;
    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}

impl Optimizer {
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut current = plan.clone();
        for rule in &self.rules {
            current = rule.apply(&current)?;
        }
        Ok(current)
    }
}
```

### Active Optimization Rules

1. **Constant Folding**: Evaluate constant expressions at plan time
   ```sql
   -- Before: SELECT 1 + 2 + x FROM t
   -- After:  SELECT 3 + x FROM t
   ```

2. **Filter Pushdown**: Move filters closer to data sources
   ```sql
   -- Before: SELECT * FROM (SELECT * FROM t) WHERE x > 10
   -- After:  SELECT * FROM (SELECT * FROM t WHERE x > 10)
   ```

3. **Projection Pushdown**: Request only needed columns
   ```sql
   -- Before: SELECT a FROM (SELECT * FROM t)
   -- After:  SELECT a FROM (SELECT a FROM t)
   ```

4. **Limit Pushdown**: Push LIMIT through operations where safe
   ```sql
   -- Before: SELECT * FROM (SELECT * FROM t) LIMIT 10
   -- After:  SELECT * FROM (SELECT * FROM t LIMIT 10)
   ```

5. **Single Row Optimization**: Optimize aggregates with no GROUP BY
   ```sql
   -- Before: SELECT COUNT(*) FROM t  (full scan)
   -- After:  Use statistics if available
   ```

### Foundation: Cost-Based Infrastructure

Located in `src/optimizer/`:

```rust
// src/optimizer/cost_model.rs
pub struct CostModel {
    pub cpu_cost_per_row: f64,
    pub io_cost_per_byte: f64,
    pub network_cost_per_byte: f64,
    pub hash_build_cost: f64,
    pub hash_probe_cost: f64,
}

// src/optimizer/statistics.rs
pub struct Statistics {
    pub num_rows: Option<usize>,
    pub total_byte_size: Option<usize>,
    pub column_statistics: Vec<ColumnStatistics>,
}

pub struct ColumnStatistics {
    pub null_count: Option<usize>,
    pub distinct_count: Option<usize>,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
}

// src/optimizer/cardinality.rs
pub struct CardinalityEstimator { /* ... */ }
```

This infrastructure exists but is not yet fully integrated into plan selection.

## Consequences

### Benefits of Rule-Based (Current)

1. **Predictability**: Same query always produces same plan
   ```rust
   // Debug-friendly: can trace rule applications
   for rule in &self.rules {
       println!("Applying rule: {}", rule.name());
       current = rule.apply(&current)?;
       println!("Result: {:?}", current);
   }
   ```

2. **No Statistics Dependency**: Works without table statistics
   - CSV files have no statistics
   - New tables work immediately
   - No stale statistics issues

3. **Fast Optimization**: Linear pass through rules
   - O(rules × plan_size)
   - No plan enumeration
   - Negligible optimization time

4. **Easy Debugging**: Can trace each transformation
   ```
   Original: Scan → Project → Filter
   After ConstantFolding: Scan → Project → Filter (literals folded)
   After FilterPushdown: Scan → Filter → Project
   After ProjectionPushdown: Scan(projected) → Filter → Project
   ```

5. **Safe Transformations**: Rules only apply when always beneficial

### Costs of Rule-Based

1. **Misses Some Optimizations**: Cannot choose between:
   - Hash join vs. sort-merge join (needs cardinality)
   - Build side selection (needs row counts)
   - Index selection (needs selectivity)

2. **Fixed Rule Order**: Rule interactions may not be optimal
   ```rust
   // Current order matters:
   // 1. Constant folding (simplify expressions first)
   // 2. Filter pushdown (filters closer to source)
   // 3. Projection pushdown (reduce column set)
   ```

3. **No Adaptive Optimization**: Cannot adjust based on runtime statistics

### Future: Cost-Based Extensions

The infrastructure supports future cost-based features:

**Join Order Optimization**:
```rust
// Future: Enumerate join orders, pick lowest cost
fn optimize_join_order(tables: Vec<LogicalPlan>) -> LogicalPlan {
    let permutations = generate_join_orders(&tables);
    permutations
        .into_iter()
        .min_by_key(|plan| self.estimate_cost(plan))
        .unwrap()
}
```

**Build Side Selection**:
```rust
// Future: Choose build side based on cardinality
fn select_hash_join_build_side(
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> BuildSide {
    let left_rows = self.estimate_cardinality(left);
    let right_rows = self.estimate_cardinality(right);
    if left_rows < right_rows { BuildSide::Left } else { BuildSide::Right }
}
```

**Statistics Collection**:
```rust
// Future: ANALYZE command
fn analyze_table(&self, table: &str) -> Result<Statistics> {
    let batches = self.scan_table(table)?;
    Statistics {
        num_rows: Some(count_rows(&batches)),
        column_statistics: compute_column_stats(&batches),
    }
}
```

### Rule Implementation Pattern

```rust
pub struct FilterPushdownRule;

impl OptimizerRule for FilterPushdownRule {
    fn name(&self) -> &str { "FilterPushdown" }

    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // Push filter through projection
            LogicalPlan::Filter { input, predicate } => {
                match input.as_ref() {
                    LogicalPlan::Projection { input: proj_input, .. } => {
                        // Move filter below projection
                        Ok(LogicalPlan::Projection {
                            input: Box::new(LogicalPlan::Filter {
                                input: proj_input.clone(),
                                predicate: predicate.clone(),
                            }),
                            ..
                        })
                    }
                    _ => Ok(plan.clone()),
                }
            }
            // Recursively apply to children
            _ => self.apply_to_children(plan),
        }
    }
}
```

### Comparison with Alternatives

| Aspect | Rule-Based (Blaze) | Cost-Based |
|--------|-------------------|------------|
| Optimization time | Fast (linear) | Slower (exponential worst case) |
| Plan quality | Good for common cases | Optimal with good stats |
| Statistics needed | No | Yes |
| Predictability | Deterministic | May vary with stats |
| Implementation | Simpler | Complex |
| Debugging | Easy to trace | Hard to explain choices |
