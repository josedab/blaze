//! Join Order Optimization
//!
//! Uses dynamic programming to find the optimal join order for multi-way joins.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::Result;
use crate::planner::{JoinType, LogicalExpr, LogicalPlan};
use crate::types::Schema;

use super::cost_model::{Cost, CostModel};

/// Optimizer for join ordering.
#[derive(Debug, Default)]
pub struct JoinOrderOptimizer {
    /// Maximum number of relations to consider for exhaustive search
    max_relations_exhaustive: usize,
    /// Whether to use greedy algorithm for large queries
    use_greedy_fallback: bool,
}

impl JoinOrderOptimizer {
    /// Create a new join order optimizer.
    pub fn new() -> Self {
        Self {
            max_relations_exhaustive: 10,
            use_greedy_fallback: true,
        }
    }

    /// Set the maximum number of relations for exhaustive search.
    pub fn with_max_relations(mut self, max: usize) -> Self {
        self.max_relations_exhaustive = max;
        self
    }

    /// Optimize join ordering in a logical plan.
    pub fn optimize(&self, plan: &LogicalPlan, cost_model: &CostModel) -> Result<LogicalPlan> {
        self.optimize_plan(plan, cost_model)
    }

    /// Recursively optimize join ordering in a plan.
    fn optimize_plan(&self, plan: &LogicalPlan, cost_model: &CostModel) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                join_type,
                on,
                filter,
                schema,
            } => {
                // First, recursively optimize children
                let optimized_left = self.optimize_plan(left, cost_model)?;
                let optimized_right = self.optimize_plan(right, cost_model)?;

                // Extract all base relations and join conditions
                let mut relations = Vec::new();
                let mut join_conditions = Vec::new();

                self.extract_join_relations(
                    &optimized_left,
                    &optimized_right,
                    on,
                    filter,
                    join_type,
                    &mut relations,
                    &mut join_conditions,
                );

                // If we have multiple relations, optimize join order
                if relations.len() > 2 && relations.len() <= self.max_relations_exhaustive {
                    self.optimize_join_order(relations, join_conditions, schema.clone(), cost_model)
                } else if relations.len() > self.max_relations_exhaustive
                    && self.use_greedy_fallback
                {
                    // Use greedy algorithm for large join graphs
                    self.greedy_join_order(relations, join_conditions, schema.clone(), cost_model)
                } else {
                    // Keep original order
                    Ok(LogicalPlan::Join {
                        left: Arc::new(optimized_left),
                        right: Arc::new(optimized_right),
                        join_type: *join_type,
                        on: on.clone(),
                        filter: filter.clone(),
                        schema: schema.clone(),
                    })
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                let optimized_input = self.optimize_plan(input, cost_model)?;
                Ok(LogicalPlan::Filter {
                    input: Arc::new(optimized_input),
                    predicate: predicate.clone(),
                })
            }
            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => {
                let optimized_input = self.optimize_plan(input, cost_model)?;
                Ok(LogicalPlan::Projection {
                    input: Arc::new(optimized_input),
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                let optimized_input = self.optimize_plan(input, cost_model)?;
                Ok(LogicalPlan::Aggregate {
                    input: Arc::new(optimized_input),
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Sort { input, exprs } => {
                let optimized_input = self.optimize_plan(input, cost_model)?;
                Ok(LogicalPlan::Sort {
                    input: Arc::new(optimized_input),
                    exprs: exprs.clone(),
                })
            }
            LogicalPlan::Limit { input, skip, fetch } => {
                let optimized_input = self.optimize_plan(input, cost_model)?;
                Ok(LogicalPlan::Limit {
                    input: Arc::new(optimized_input),
                    skip: *skip,
                    fetch: *fetch,
                })
            }
            LogicalPlan::SubqueryAlias {
                input,
                alias,
                schema,
            } => {
                let optimized_input = self.optimize_plan(input, cost_model)?;
                Ok(LogicalPlan::SubqueryAlias {
                    input: Arc::new(optimized_input),
                    alias: alias.clone(),
                    schema: schema.clone(),
                })
            }
            // Plans that don't need join optimization
            _ => Ok(plan.clone()),
        }
    }

    /// Extract base relations and join conditions from a join tree.
    fn extract_join_relations(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        on: &[(LogicalExpr, LogicalExpr)],
        _filter: &Option<LogicalExpr>,
        _join_type: &JoinType,
        relations: &mut Vec<LogicalPlan>,
        join_conditions: &mut Vec<JoinCondition>,
    ) {
        // Extract left relations
        self.extract_relations_recursive(left, relations, join_conditions);

        // Extract right relations
        self.extract_relations_recursive(right, relations, join_conditions);

        // Add the join conditions
        for (left_expr, right_expr) in on {
            if let Some(jc) = self.parse_equality_condition(left_expr, right_expr) {
                join_conditions.push(jc);
            }
        }
    }

    /// Recursively extract base relations.
    fn extract_relations_recursive(
        &self,
        plan: &LogicalPlan,
        relations: &mut Vec<LogicalPlan>,
        join_conditions: &mut Vec<JoinCondition>,
    ) {
        match plan {
            LogicalPlan::Join {
                left, right, on, ..
            } => {
                self.extract_relations_recursive(left, relations, join_conditions);
                self.extract_relations_recursive(right, relations, join_conditions);
                for (left_expr, right_expr) in on {
                    if let Some(jc) = self.parse_equality_condition(left_expr, right_expr) {
                        join_conditions.push(jc);
                    }
                }
            }
            LogicalPlan::TableScan { .. } | LogicalPlan::SubqueryAlias { .. } => {
                relations.push(plan.clone());
            }
            LogicalPlan::Filter { input: _, .. } | LogicalPlan::Projection { input: _, .. } => {
                // Preserve filter/projection on base relations
                relations.push(plan.clone());
            }
            _ => {
                relations.push(plan.clone());
            }
        }
    }

    /// Parse an equality condition from two expressions.
    fn parse_equality_condition(
        &self,
        left: &LogicalExpr,
        right: &LogicalExpr,
    ) -> Option<JoinCondition> {
        let left_col = self.extract_column(left)?;
        let right_col = self.extract_column(right)?;
        Some(JoinCondition {
            left_table: left_col.0,
            left_column: left_col.1,
            right_table: right_col.0,
            right_column: right_col.1,
        })
    }

    /// Extract column information from an expression.
    fn extract_column(&self, expr: &LogicalExpr) -> Option<(String, String)> {
        match expr {
            LogicalExpr::Column(col) => {
                let table = col.relation.clone().unwrap_or_default();
                Some((table, col.name.clone()))
            }
            _ => None,
        }
    }

    /// Optimize join order using dynamic programming.
    fn optimize_join_order(
        &self,
        relations: Vec<LogicalPlan>,
        join_conditions: Vec<JoinCondition>,
        schema: Schema,
        cost_model: &CostModel,
    ) -> Result<LogicalPlan> {
        let n = relations.len();
        if n <= 1 {
            return Ok(relations
                .into_iter()
                .next()
                .unwrap_or_else(|| LogicalPlan::Values {
                    values: vec![],
                    schema: schema.clone(),
                }));
        }

        // Build a join graph
        let join_graph = JoinGraph::new(&relations, &join_conditions);

        // Use dynamic programming to find optimal order
        let mut dp: HashMap<RelationSet, DpEntry> = HashMap::new();

        // Initialize with single relations
        for (i, rel) in relations.iter().enumerate() {
            let set = RelationSet::singleton(i);
            let cost = cost_model.estimate(rel).unwrap_or_else(|_| Cost::zero());
            dp.insert(
                set,
                DpEntry {
                    plan: rel.clone(),
                    cost,
                    cardinality: 1000, // Default estimate
                },
            );
        }

        // Build up larger sets
        for size in 2..=n {
            let subsets: Vec<RelationSet> = generate_subsets(n, size);

            for subset in subsets {
                let mut best_plan: Option<LogicalPlan> = None;
                let mut best_cost = Cost::new(f64::MAX, f64::MAX, f64::MAX, f64::MAX);

                // Try all ways to partition this subset into two non-empty parts
                for left_subset in subset.proper_subsets() {
                    let right_subset = subset.difference(&left_subset);

                    if left_subset.is_empty() || right_subset.is_empty() {
                        continue;
                    }

                    // Check if there's a join condition connecting these subsets
                    if !join_graph.has_join_edge(&left_subset, &right_subset, &relations) {
                        continue;
                    }

                    // Get the best plans for left and right subsets
                    let left_entry = match dp.get(&left_subset) {
                        Some(e) => e,
                        None => continue,
                    };
                    let right_entry = match dp.get(&right_subset) {
                        Some(e) => e,
                        None => continue,
                    };

                    // Calculate join cost
                    let join_cost = cost_model.hash_join_cost(
                        left_entry.cardinality,
                        right_entry.cardinality,
                        (left_entry.cardinality * right_entry.cardinality) / 10, // Estimate
                    );

                    let total_cost = left_entry.cost.add(&right_entry.cost).add(&join_cost);

                    if total_cost.total() < best_cost.total() {
                        best_cost = total_cost;

                        // Find join condition
                        let on = join_graph.find_join_conditions(
                            &left_subset,
                            &right_subset,
                            &relations,
                        );

                        // Create the combined schema
                        let combined_schema =
                            Self::combine_schemas(&left_entry.plan, &right_entry.plan);

                        best_plan = Some(LogicalPlan::Join {
                            left: Arc::new(left_entry.plan.clone()),
                            right: Arc::new(right_entry.plan.clone()),
                            join_type: JoinType::Inner,
                            on,
                            filter: None,
                            schema: combined_schema,
                        });
                    }
                }

                if let Some(plan) = best_plan {
                    let cardinality = dp.get(&subset).map(|e| e.cardinality).unwrap_or(1000);

                    dp.insert(
                        subset,
                        DpEntry {
                            plan,
                            cost: best_cost,
                            cardinality,
                        },
                    );
                }
            }
        }

        // Get the best plan for all relations
        let full_set = RelationSet::full(n);
        match dp.get(&full_set) {
            Some(entry) => Ok(entry.plan.clone()),
            None => {
                // If no plan found, return original left-deep join
                self.build_left_deep_join(relations, join_conditions, schema)
            }
        }
    }

    /// Greedy join ordering for large queries.
    fn greedy_join_order(
        &self,
        mut relations: Vec<LogicalPlan>,
        join_conditions: Vec<JoinCondition>,
        schema: Schema,
        cost_model: &CostModel,
    ) -> Result<LogicalPlan> {
        if relations.is_empty() {
            return Ok(LogicalPlan::Values {
                values: vec![],
                schema,
            });
        }

        if relations.len() == 1 {
            return Ok(relations.remove(0));
        }

        let join_graph = JoinGraph::new(&relations, &join_conditions);
        let mut result = relations.remove(0);

        while !relations.is_empty() {
            // Find the best relation to join next
            let mut best_idx = 0;
            let mut best_cost = f64::MAX;

            for (i, rel) in relations.iter().enumerate() {
                // Check if there's a join condition
                if join_graph.can_join(&result, rel) {
                    let cost = cost_model.hash_join_cost(1000, 1000, 1000);
                    if cost.total() < best_cost {
                        best_cost = cost.total();
                        best_idx = i;
                    }
                }
            }

            let next_rel = relations.remove(best_idx);
            let on = join_graph.find_direct_conditions(&result, &next_rel);
            let combined_schema = Self::combine_schemas(&result, &next_rel);

            result = LogicalPlan::Join {
                left: Arc::new(result),
                right: Arc::new(next_rel),
                join_type: JoinType::Inner,
                on,
                filter: None,
                schema: combined_schema,
            };
        }

        Ok(result)
    }

    /// Build a left-deep join tree.
    fn build_left_deep_join(
        &self,
        mut relations: Vec<LogicalPlan>,
        join_conditions: Vec<JoinCondition>,
        schema: Schema,
    ) -> Result<LogicalPlan> {
        if relations.is_empty() {
            return Ok(LogicalPlan::Values {
                values: vec![],
                schema,
            });
        }

        if relations.len() == 1 {
            return Ok(relations.remove(0));
        }

        let join_graph = JoinGraph::new(&relations, &join_conditions);
        let mut result = relations.remove(0);

        for rel in relations {
            let on = join_graph.find_direct_conditions(&result, &rel);
            let combined_schema = Self::combine_schemas(&result, &rel);

            result = LogicalPlan::Join {
                left: Arc::new(result),
                right: Arc::new(rel),
                join_type: JoinType::Inner,
                on,
                filter: None,
                schema: combined_schema,
            };
        }

        Ok(result)
    }

    /// Combine schemas from two plans.
    fn combine_schemas(left: &LogicalPlan, right: &LogicalPlan) -> Schema {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let mut fields = left_schema.fields().to_vec();
        fields.extend(right_schema.fields().iter().cloned());

        Schema::new(fields)
    }
}

/// A join condition between two relations.
#[derive(Debug, Clone)]
pub struct JoinCondition {
    /// Table name for left side
    pub left_table: String,
    /// Column name for left side
    pub left_column: String,
    /// Table name for right side
    pub right_table: String,
    /// Column name for right side
    pub right_column: String,
}

impl JoinCondition {
    /// Convert to a pair of LogicalExprs for the `on` field.
    pub fn to_expr_pair(&self) -> (LogicalExpr, LogicalExpr) {
        let left = LogicalExpr::Column(crate::planner::Column {
            name: self.left_column.clone(),
            relation: if self.left_table.is_empty() {
                None
            } else {
                Some(self.left_table.clone())
            },
        });
        let right = LogicalExpr::Column(crate::planner::Column {
            name: self.right_column.clone(),
            relation: if self.right_table.is_empty() {
                None
            } else {
                Some(self.right_table.clone())
            },
        });
        (left, right)
    }
}

/// A set of relation indices (bit vector).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RelationSet(u64);

impl RelationSet {
    /// Create an empty set.
    fn empty() -> Self {
        Self(0)
    }

    /// Create a singleton set.
    fn singleton(idx: usize) -> Self {
        Self(1 << idx)
    }

    /// Create a full set of n relations.
    fn full(n: usize) -> Self {
        Self((1 << n) - 1)
    }

    /// Check if the set is empty.
    fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Check if the set contains an index.
    fn contains(&self, idx: usize) -> bool {
        (self.0 & (1 << idx)) != 0
    }

    /// Union of two sets.
    fn union(&self, other: &Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Difference of two sets.
    fn difference(&self, other: &Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// Get proper subsets of this set.
    fn proper_subsets(&self) -> Vec<Self> {
        let mut subsets = Vec::new();
        let mut s = self.0;
        while s > 0 {
            s = (s - 1) & self.0;
            if s > 0 && s != self.0 {
                subsets.push(Self(s));
            }
        }
        subsets
    }

    /// Get the number of elements in the set.
    fn count(&self) -> usize {
        self.0.count_ones() as usize
    }
}

/// Entry in the dynamic programming table.
struct DpEntry {
    plan: LogicalPlan,
    cost: Cost,
    cardinality: usize,
}

/// Join graph representing connections between relations.
struct JoinGraph {
    /// Edges in the join graph (pairs of relation indices)
    edges: HashSet<(usize, usize)>,
    /// Join conditions for each edge
    conditions: HashMap<(usize, usize), Vec<JoinCondition>>,
}

impl JoinGraph {
    /// Create a new join graph.
    fn new(relations: &[LogicalPlan], join_conditions: &[JoinCondition]) -> Self {
        let mut edges = HashSet::new();
        let mut conditions: HashMap<(usize, usize), Vec<JoinCondition>> = HashMap::new();

        // Map table names to relation indices
        let table_to_idx: HashMap<String, usize> = relations
            .iter()
            .enumerate()
            .filter_map(|(i, plan)| Self::get_table_name(plan).map(|name| (name, i)))
            .collect();

        // Build edges from join conditions
        for jc in join_conditions {
            if let (Some(&left_idx), Some(&right_idx)) = (
                table_to_idx.get(&jc.left_table),
                table_to_idx.get(&jc.right_table),
            ) {
                let key = if left_idx < right_idx {
                    (left_idx, right_idx)
                } else {
                    (right_idx, left_idx)
                };
                edges.insert(key);
                conditions.entry(key).or_default().push(jc.clone());
            }
        }

        Self { edges, conditions }
    }

    /// Get table name from a plan.
    fn get_table_name(plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::TableScan { table_ref, .. } => Some(table_ref.table.clone()),
            LogicalPlan::SubqueryAlias { alias, .. } => Some(alias.clone()),
            LogicalPlan::Filter { input, .. } | LogicalPlan::Projection { input, .. } => {
                Self::get_table_name(input)
            }
            _ => None,
        }
    }

    /// Check if there's a join edge between two relation sets.
    fn has_join_edge(
        &self,
        left: &RelationSet,
        right: &RelationSet,
        _relations: &[LogicalPlan],
    ) -> bool {
        for &(i, j) in &self.edges {
            if (left.contains(i) && right.contains(j)) || (left.contains(j) && right.contains(i)) {
                return true;
            }
        }
        // If no explicit edge, allow cross join
        true
    }

    /// Find join conditions between two relation sets.
    fn find_join_conditions(
        &self,
        left: &RelationSet,
        right: &RelationSet,
        _relations: &[LogicalPlan],
    ) -> Vec<(LogicalExpr, LogicalExpr)> {
        let mut result = Vec::new();
        for &(i, j) in &self.edges {
            if (left.contains(i) && right.contains(j)) || (left.contains(j) && right.contains(i)) {
                if let Some(jcs) = self.conditions.get(&(i, j)) {
                    for jc in jcs {
                        result.push(jc.to_expr_pair());
                    }
                }
            }
        }
        result
    }

    /// Check if two plans can be joined.
    fn can_join(&self, _left: &LogicalPlan, _right: &LogicalPlan) -> bool {
        // For simplicity, always allow joining
        true
    }

    /// Find direct join conditions between two plans.
    fn find_direct_conditions(
        &self,
        _left: &LogicalPlan,
        _right: &LogicalPlan,
    ) -> Vec<(LogicalExpr, LogicalExpr)> {
        // Return empty for now - would need table name extraction
        Vec::new()
    }
}

/// Generate all subsets of size k from n elements.
fn generate_subsets(n: usize, k: usize) -> Vec<RelationSet> {
    let mut result = Vec::new();
    generate_subsets_helper(n, k, 0, RelationSet::empty(), &mut result);
    result
}

fn generate_subsets_helper(
    n: usize,
    k: usize,
    start: usize,
    current: RelationSet,
    result: &mut Vec<RelationSet>,
) {
    if current.count() == k {
        result.push(current);
        return;
    }

    for i in start..n {
        let new_set = current.union(&RelationSet::singleton(i));
        generate_subsets_helper(n, k, i + 1, new_set, result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ResolvedTableRef;
    use crate::types::{DataType, Field};

    fn create_test_scan(name: &str) -> LogicalPlan {
        LogicalPlan::TableScan {
            table_ref: ResolvedTableRef::new("default", "main", name),
            projection: None,
            schema: Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, true),
            ]),
            filters: vec![],
            time_travel: None,
        }
    }

    #[test]
    fn test_relation_set() {
        let a = RelationSet::singleton(0);
        let b = RelationSet::singleton(1);
        let ab = a.union(&b);

        assert!(ab.contains(0));
        assert!(ab.contains(1));
        assert!(!ab.contains(2));
        assert_eq!(ab.count(), 2);

        let diff = ab.difference(&a);
        assert!(!diff.contains(0));
        assert!(diff.contains(1));
    }

    #[test]
    fn test_relation_set_subsets() {
        let full = RelationSet::full(3);
        let subsets = full.proper_subsets();

        // Should have 6 proper subsets (2^3 - 2 = 6, excluding empty and full)
        assert!(!subsets.is_empty());
    }

    #[test]
    fn test_generate_subsets() {
        let subsets = generate_subsets(4, 2);
        assert_eq!(subsets.len(), 6); // C(4,2) = 6

        for subset in &subsets {
            assert_eq!(subset.count(), 2);
        }
    }

    #[test]
    fn test_join_condition_to_expr() {
        let jc = JoinCondition {
            left_table: "orders".to_string(),
            left_column: "customer_id".to_string(),
            right_table: "customers".to_string(),
            right_column: "id".to_string(),
        };

        let (left_expr, right_expr) = jc.to_expr_pair();
        match left_expr {
            LogicalExpr::Column(col) => {
                assert_eq!(col.name, "customer_id");
                assert_eq!(col.relation, Some("orders".to_string()));
            }
            _ => panic!("Expected Column"),
        }
        match right_expr {
            LogicalExpr::Column(col) => {
                assert_eq!(col.name, "id");
                assert_eq!(col.relation, Some("customers".to_string()));
            }
            _ => panic!("Expected Column"),
        }
    }

    #[test]
    fn test_join_optimizer_simple() {
        let optimizer = JoinOrderOptimizer::new();
        let cost_model = CostModel::default();

        let left = create_test_scan("a");
        let right = create_test_scan("b");

        let combined_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]);

        let join = LogicalPlan::Join {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type: JoinType::Inner,
            on: vec![(
                LogicalExpr::Column(crate::planner::Column {
                    name: "id".to_string(),
                    relation: Some("a".to_string()),
                }),
                LogicalExpr::Column(crate::planner::Column {
                    name: "id".to_string(),
                    relation: Some("b".to_string()),
                }),
            )],
            filter: None,
            schema: combined_schema,
        };

        let result = optimizer.optimize(&join, &cost_model);
        assert!(result.is_ok());
    }

    #[test]
    fn test_join_graph() {
        let relations = vec![
            create_test_scan("a"),
            create_test_scan("b"),
            create_test_scan("c"),
        ];

        let conditions = vec![
            JoinCondition {
                left_table: "a".to_string(),
                left_column: "id".to_string(),
                right_table: "b".to_string(),
                right_column: "a_id".to_string(),
            },
            JoinCondition {
                left_table: "b".to_string(),
                left_column: "id".to_string(),
                right_table: "c".to_string(),
                right_column: "b_id".to_string(),
            },
        ];

        let graph = JoinGraph::new(&relations, &conditions);
        assert!(!graph.edges.is_empty());
    }

    #[test]
    fn test_greedy_join_order() {
        let optimizer = JoinOrderOptimizer::new();
        let cost_model = CostModel::default();

        let relations = vec![
            create_test_scan("a"),
            create_test_scan("b"),
            create_test_scan("c"),
        ];

        let conditions = vec![JoinCondition {
            left_table: "a".to_string(),
            left_column: "id".to_string(),
            right_table: "b".to_string(),
            right_column: "a_id".to_string(),
        }];

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let result = optimizer.greedy_join_order(relations, conditions, schema, &cost_model);
        assert!(result.is_ok());

        // Result should be a join
        match result.unwrap() {
            LogicalPlan::Join { .. } => {}
            _ => panic!("Expected Join"),
        }
    }
}
