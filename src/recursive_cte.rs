//! Recursive CTE execution support.
//!
//! Implements WITH RECURSIVE by iteratively executing the recursive term
//! and unioning results until a fixpoint is reached.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// Configuration for recursive CTE execution.
#[derive(Debug, Clone)]
pub struct RecursiveCteConfig {
    /// Maximum number of iterations before aborting (cycle protection)
    pub max_iterations: usize,
    /// Maximum total rows produced before aborting
    pub max_rows: usize,
}

impl Default for RecursiveCteConfig {
    fn default() -> Self {
        Self {
            max_iterations: 1000,
            max_rows: 1_000_000,
        }
    }
}

/// Execute a recursive CTE by iterating anchor + recursive parts.
///
/// Algorithm:
/// 1. Execute anchor query → working table (iteration 0)
/// 2. Execute recursive query with working table → new rows
/// 3. Append new rows to result, replace working table
/// 4. Repeat until no new rows or max iterations
pub fn execute_recursive_cte(
    conn: &crate::Connection,
    cte_name: &str,
    anchor_sql: &str,
    recursive_sql: &str,
    config: &RecursiveCteConfig,
) -> Result<Vec<RecordBatch>> {
    use crate::storage::MemoryTable;

    // Step 1: Execute anchor query
    let anchor_result = conn.query(anchor_sql)?;
    let total_anchor_rows: usize = anchor_result.iter().map(|b| b.num_rows()).sum();
    if anchor_result.is_empty() || total_anchor_rows == 0 {
        return Ok(anchor_result);
    }

    let schema = anchor_result[0].schema();
    let blaze_schema = Schema::from_arrow(&schema)?;

    // Initialize working table and result accumulator
    let mut all_results: Vec<RecordBatch> = anchor_result.clone();
    let mut working_table_data: Vec<RecordBatch> = anchor_result;
    let mut total_rows: usize = all_results.iter().map(|b| b.num_rows()).sum();

    for iteration in 0..config.max_iterations {
        // Register working table as a temp table
        let working_table = MemoryTable::new(blaze_schema.clone(), working_table_data.clone());
        conn.register_table(cte_name, Arc::new(working_table))?;

        // Execute recursive term
        let new_rows = conn.query(recursive_sql)?;

        // Clean up the temp table
        let _ = conn.deregister_table(cte_name);

        // Check if we got new rows
        let new_row_count: usize = new_rows.iter().map(|b| b.num_rows()).sum();
        if new_row_count == 0 {
            break; // Fixpoint reached
        }

        total_rows += new_row_count;
        if total_rows > config.max_rows {
            return Err(BlazeError::execution(format!(
                "Recursive CTE '{}' exceeded max rows limit ({}) at iteration {}",
                cte_name, config.max_rows, iteration
            )));
        }

        all_results.extend(new_rows.clone());
        working_table_data = new_rows;
    }

    Ok(all_results)
}

/// Parse a recursive CTE query into anchor and recursive parts.
/// Assumes the format: WITH RECURSIVE name AS (anchor UNION ALL recursive) SELECT ...
/// Returns (cte_name, anchor_sql, recursive_sql, main_query_sql).
pub fn parse_recursive_cte(sql: &str) -> Option<(String, String, String, String)> {
    let upper = sql.to_uppercase();

    // Check for WITH RECURSIVE
    if !upper.starts_with("WITH RECURSIVE") && !upper.contains("WITH RECURSIVE") {
        return None;
    }

    // Find the CTE name after WITH RECURSIVE
    let _after_recursive = sql.trim_start_matches(|c: char| {
        !c.is_alphabetic() || "WITHRECURSIVE".contains(c.to_ascii_uppercase())
    });

    // Simple parsing: find "AS (" then match the closing ")"
    let as_pos = upper.find(" AS (")?;
    let cte_name = sql[15..as_pos].trim().to_string(); // 15 = "WITH RECURSIVE ".len()

    // Find the UNION ALL separator within the CTE definition
    let cte_start = as_pos + 5; // skip " AS ("
    let mut paren_depth = 1;
    let mut cte_end = cte_start;

    for (i, ch) in sql[cte_start..].char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth == 0 {
                    cte_end = cte_start + i;
                    break;
                }
            }
            _ => {}
        }
    }

    let cte_body = &sql[cte_start..cte_end];
    let cte_upper = cte_body.to_uppercase();

    // Split on UNION ALL
    let union_pos = cte_upper.find("UNION ALL")?;
    let anchor = cte_body[..union_pos].trim().to_string();
    let recursive = cte_body[union_pos + 9..].trim().to_string();

    // Main query is everything after the closing paren
    let main_query = sql[cte_end + 1..].trim().to_string();

    Some((cte_name, anchor, recursive, main_query))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_recursive_cte() {
        let sql = "WITH RECURSIVE cnt AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT * FROM cnt";
        let result = parse_recursive_cte(sql);
        assert!(result.is_some());
        let (name, anchor, recursive, main) = result.unwrap();
        assert_eq!(name, "cnt");
        assert_eq!(anchor, "SELECT 1 AS n");
        assert_eq!(recursive, "SELECT n + 1 FROM cnt WHERE n < 5");
        assert_eq!(main, "SELECT * FROM cnt");
    }

    #[test]
    fn test_parse_non_recursive() {
        let sql = "WITH t AS (SELECT 1) SELECT * FROM t";
        assert!(parse_recursive_cte(sql).is_none());
    }

    #[test]
    fn test_recursive_cte_execution() {
        let conn = crate::Connection::in_memory().unwrap();

        // Execute: generate numbers 1 to 5
        let result = execute_recursive_cte(
            &conn,
            "cnt",
            "SELECT 1 AS n",
            "SELECT n + 1 AS n FROM cnt WHERE n < 5",
            &RecursiveCteConfig::default(),
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_recursive_cte_max_iterations() {
        let conn = crate::Connection::in_memory().unwrap();

        // Infinite recursion (no WHERE clause)
        let config = RecursiveCteConfig {
            max_iterations: 5,
            max_rows: 100,
        };
        let result = execute_recursive_cte(
            &conn,
            "inf",
            "SELECT 1 AS n",
            "SELECT n + 1 AS n FROM inf",
            &config,
        );

        // Should either reach max_iterations or max_rows
        assert!(result.is_err() || result.unwrap().len() <= 6);
    }

    #[test]
    fn test_recursive_cte_config() {
        let config = RecursiveCteConfig::default();
        assert_eq!(config.max_iterations, 1000);
        assert_eq!(config.max_rows, 1_000_000);
    }

    #[test]
    fn test_recursive_cte_empty_anchor() {
        let conn = crate::Connection::in_memory().unwrap();

        // Anchor that returns no rows
        conn.execute("CREATE TABLE empty_t (n BIGINT)").unwrap();
        let result = execute_recursive_cte(
            &conn,
            "rcte",
            "SELECT n FROM empty_t WHERE n > 999",
            "SELECT n + 1 FROM rcte WHERE n < 5",
            &RecursiveCteConfig::default(),
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
