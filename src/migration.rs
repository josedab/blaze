//! DuckDB migration toolkit for converting DuckDB SQL to Blaze SQL.
//!
//! This module provides tools for migrating from DuckDB to Blaze by converting
//! DuckDB-specific SQL syntax and types to Blaze-compatible alternatives.

use crate::error::{BlazeError, Result};
use std::collections::HashMap;

/// Converts DuckDB-specific SQL to Blaze SQL.
pub struct DuckDbDialectConverter;

impl DuckDbDialectConverter {
    /// Convert a DuckDB SQL query to Blaze SQL.
    pub fn convert(sql: &str) -> Result<String> {
        let mut result = sql.to_string();

        // Convert double-colon casts (::INT) to CAST(x AS INT)
        result = Self::convert_double_colon_casts(&result);

        // Convert CREATE OR REPLACE TABLE to DROP + CREATE
        result = Self::convert_create_or_replace(&result);

        // Convert REGEXP_MATCHES to LIKE approximation
        result = Self::convert_regexp_matches(&result);

        // Convert EPOCH_MS() to equivalent
        result = Self::convert_epoch_ms(&result);

        // Convert LIST_AGG to GROUP_CONCAT
        result = Self::convert_list_agg(&result);

        // Convert QUALIFY clause to subquery
        result = Self::convert_qualify(&result)?;

        Ok(result)
    }

    /// Convert double-colon type casts to standard CAST syntax.
    fn convert_double_colon_casts(sql: &str) -> String {
        // Simple pattern: column::TYPE → CAST(column AS TYPE)
        let mut result = String::new();
        let mut chars = sql.chars().peekable();
        let mut buffer = String::new();

        while let Some(ch) = chars.next() {
            if ch == ':' && chars.peek() == Some(&':') {
                chars.next(); // consume second ':'

                // Extract the type name (alphanumeric + underscore)
                let mut type_name = String::new();
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_alphanumeric() || next_ch == '_' {
                        type_name.push(next_ch);
                        chars.next();
                    } else {
                        break;
                    }
                }

                if !type_name.is_empty() && !buffer.is_empty() {
                    // Find the expression to cast (simple heuristic: take last token)
                    let expr = buffer.trim().split_whitespace().last().unwrap_or("");
                    let prefix_len = buffer.len() - expr.len();
                    result.push_str(&buffer[..prefix_len]);
                    result.push_str(&format!("CAST({} AS {})", expr, type_name.to_uppercase()));
                    buffer.clear();
                } else {
                    result.push_str(&buffer);
                    result.push_str("::");
                    result.push_str(&type_name);
                    buffer.clear();
                }
            } else {
                buffer.push(ch);
            }
        }

        result.push_str(&buffer);
        result
    }

    /// Convert CREATE OR REPLACE TABLE to DROP TABLE IF EXISTS + CREATE TABLE.
    fn convert_create_or_replace(sql: &str) -> String {
        let lower = sql.to_lowercase();
        if let Some(pos) = lower.find("create or replace table") {
            // Extract table name
            let after = &sql[pos + 23..];
            if let Some(table_name) = after.trim().split_whitespace().next() {
                let create_clause = &sql[pos..];
                let drop = format!("DROP TABLE IF EXISTS {};\n", table_name);
                let create = create_clause.replace("CREATE OR REPLACE TABLE", "CREATE TABLE");
                return format!("{}{}{}", &sql[..pos], drop, create);
            }
        }
        sql.to_string()
    }

    /// Convert REGEXP_MATCHES to LIKE approximation.
    fn convert_regexp_matches(sql: &str) -> String {
        // REGEXP_MATCHES(column, pattern) → column LIKE '%pattern%'
        // This is a simplification - not all regexes can be converted to LIKE
        sql.replace(
            "REGEXP_MATCHES",
            "LIKE /* WARNING: simplified from REGEXP_MATCHES */",
        )
    }

    /// Convert EPOCH_MS() to milliseconds since epoch.
    fn convert_epoch_ms(sql: &str) -> String {
        // DuckDB's EPOCH_MS(timestamp) → extract epoch milliseconds
        // Approximate: use UNIX_TIMESTAMP or equivalent
        sql.replace("EPOCH_MS(", "CAST(EXTRACT(EPOCH FROM ")
            .replace("EPOCH_MS (", "CAST(EXTRACT(EPOCH FROM ")
            + " * 1000 AS BIGINT)"
    }

    /// Convert LIST_AGG to GROUP_CONCAT or error.
    fn convert_list_agg(sql: &str) -> String {
        if sql.to_uppercase().contains("LIST_AGG") {
            sql.replace(
                "LIST_AGG",
                "GROUP_CONCAT /* WARNING: LIST_AGG not fully supported */",
            )
        } else {
            sql.to_string()
        }
    }

    /// Convert QUALIFY clause to subquery rewrite.
    fn convert_qualify(sql: &str) -> Result<String> {
        let lower = sql.to_lowercase();
        if lower.contains("qualify") {
            // QUALIFY is a window function filter - needs subquery rewrite
            // For now, return an error with suggestion
            return Err(BlazeError::Analysis {
                message: "QUALIFY clause not supported. Rewrite using a subquery with WHERE on window function results.".to_string(),
            });
        }
        Ok(sql.to_string())
    }
}

/// Converts DuckDB schema DDL to Blaze DDL.
pub struct SchemaConverter;

impl SchemaConverter {
    /// Convert a DuckDB CREATE TABLE statement to Blaze DDL.
    pub fn convert_create_table(ddl: &str) -> Result<String> {
        let mut result = ddl.to_string();

        // Map DuckDB types to Blaze types
        result = Self::map_types(&result);

        Ok(result)
    }

    /// Map DuckDB types to Blaze types.
    fn map_types(ddl: &str) -> String {
        let type_map = Self::get_type_mappings();
        let mut result = ddl.to_string();

        for (duckdb_type, blaze_type) in type_map {
            // Simple word boundary replacement - handle common cases
            // Handle types with parentheses (like STRUCT(...))
            if result.contains(&format!(" {}(", duckdb_type)) {
                // Find and replace the whole type including its parameters
                result = result.replace(
                    &format!(" {}(", duckdb_type),
                    &format!(" {}/* WAS: {}(", blaze_type, duckdb_type),
                );
            }
            result = result.replace(&format!(" {} ", duckdb_type), &format!(" {} ", blaze_type));
            result = result.replace(&format!("({}", duckdb_type), &format!("({}", blaze_type));
            result = result.replace(&format!(" {},", duckdb_type), &format!(" {},", blaze_type));
            result = result.replace(&format!(" {})", duckdb_type), &format!(" {})", blaze_type));
        }

        result
    }

    /// Get type mappings from DuckDB to Blaze.
    fn get_type_mappings() -> HashMap<String, &'static str> {
        let mut map = HashMap::new();

        // Integer types
        map.insert(
            "HUGEINT".to_string(),
            "BIGINT /* WARNING: HUGEINT mapped to BIGINT - may overflow */",
        );
        map.insert(
            "UTINYINT".to_string(),
            "TINYINT /* WARNING: UTINYINT mapped to TINYINT - signedness differs */",
        );
        map.insert(
            "USMALLINT".to_string(),
            "SMALLINT /* WARNING: USMALLINT mapped to SMALLINT - signedness differs */",
        );
        map.insert(
            "UINTEGER".to_string(),
            "INTEGER /* WARNING: UINTEGER mapped to INTEGER - signedness differs */",
        );
        map.insert(
            "UBIGINT".to_string(),
            "BIGINT /* WARNING: UBIGINT mapped to BIGINT - signedness differs */",
        );

        // Complex types
        map.insert(
            "BLOB".to_string(),
            "BINARY /* WARNING: BLOB mapped to BINARY */",
        );
        map.insert(
            "MAP".to_string(),
            "VARCHAR /* ERROR: MAP type not supported - stored as VARCHAR */",
        );
        map.insert(
            "STRUCT".to_string(),
            "VARCHAR /* ERROR: STRUCT type not supported - stored as VARCHAR */",
        );
        map.insert(
            "LIST".to_string(),
            "VARCHAR /* ERROR: LIST type not supported - stored as VARCHAR */",
        );
        map.insert(
            "ARRAY".to_string(),
            "VARCHAR /* ERROR: ARRAY type not supported - stored as VARCHAR */",
        );

        // Time types
        map.insert(
            "TIMESTAMPTZ".to_string(),
            "TIMESTAMP /* WARNING: TIMESTAMPTZ mapped to TIMESTAMP - timezone info lost */",
        );

        map
    }

    /// Check for unsupported DuckDB features.
    pub fn check_unsupported_features(ddl: &str) -> Vec<String> {
        let mut warnings = Vec::new();

        if ddl.to_uppercase().contains("STRUCT") {
            warnings.push(
                "STRUCT types are not supported. Consider flattening or using JSON.".to_string(),
            );
        }

        if ddl.to_uppercase().contains("MAP") {
            warnings.push(
                "MAP types are not supported. Consider using JSON or multiple columns.".to_string(),
            );
        }

        if ddl.to_uppercase().contains("LIST") || ddl.to_uppercase().contains("ARRAY") {
            warnings.push(
                "LIST/ARRAY types are not supported. Consider using JSON or separate tables."
                    .to_string(),
            );
        }

        if ddl.to_uppercase().contains("ENUM") {
            warnings.push("ENUM types may not be fully supported. Consider using VARCHAR with CHECK constraints.".to_string());
        }

        warnings
    }
}

/// A report of migration results.
#[derive(Debug, Clone)]
pub struct MigrationReport {
    /// Original queries
    pub original_queries: Vec<String>,
    /// Converted queries
    pub converted_queries: Vec<String>,
    /// Warnings about potential issues
    pub warnings: Vec<String>,
    /// Errors that prevented conversion
    pub errors: Vec<String>,
}

impl MigrationReport {
    pub fn new() -> Self {
        Self {
            original_queries: Vec::new(),
            converted_queries: Vec::new(),
            warnings: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Check if migration was successful (no errors).
    pub fn is_successful(&self) -> bool {
        self.errors.is_empty()
    }

    /// Get a summary string.
    pub fn summary(&self) -> String {
        format!(
            "Migration Report: {} queries, {} warnings, {} errors",
            self.converted_queries.len(),
            self.warnings.len(),
            self.errors.len()
        )
    }
}

impl Default for MigrationReport {
    fn default() -> Self {
        Self::new()
    }
}

/// Plans and executes migration from DuckDB to Blaze.
pub struct MigrationPlanner;

impl MigrationPlanner {
    /// Plan migration for a list of SQL queries.
    pub fn plan(queries: Vec<String>) -> MigrationReport {
        let mut report = MigrationReport::new();

        for (idx, query) in queries.iter().enumerate() {
            report.original_queries.push(query.clone());

            // Check if it's a DDL statement
            let is_ddl = query.trim().to_uppercase().starts_with("CREATE TABLE");

            if is_ddl {
                // Schema conversion
                match SchemaConverter::convert_create_table(query) {
                    Ok(converted) => {
                        report.converted_queries.push(converted.clone());

                        // Check for warnings
                        let warnings = SchemaConverter::check_unsupported_features(&converted);
                        for warning in warnings {
                            report
                                .warnings
                                .push(format!("Query {}: {}", idx + 1, warning));
                        }
                    }
                    Err(e) => {
                        report.errors.push(format!("Query {}: {}", idx + 1, e));
                        report.converted_queries.push(query.clone());
                    }
                }
            } else {
                // SQL conversion
                match DuckDbDialectConverter::convert(query) {
                    Ok(converted) => {
                        report.converted_queries.push(converted.clone());

                        // Add warnings if conversion had to approximate
                        if converted.contains("WARNING:") {
                            report.warnings.push(format!(
                                "Query {}: Contains approximated conversions - review carefully",
                                idx + 1
                            ));
                        }
                    }
                    Err(e) => {
                        report.errors.push(format!("Query {}: {}", idx + 1, e));
                        report.converted_queries.push(query.clone());
                    }
                }
            }
        }

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_double_colon_cast_conversion() {
        let sql = "SELECT id::INTEGER, name FROM users";
        let converted = DuckDbDialectConverter::convert(sql).unwrap();
        assert!(converted.contains("CAST(id AS INTEGER)"));
    }

    #[test]
    fn test_create_or_replace_conversion() {
        let sql = "CREATE OR REPLACE TABLE users (id INT, name VARCHAR)";
        let converted = DuckDbDialectConverter::convert(sql).unwrap();
        assert!(converted.contains("DROP TABLE IF EXISTS users"));
        assert!(converted.contains("CREATE TABLE users"));
    }

    #[test]
    fn test_regexp_matches_conversion() {
        let sql = "SELECT * FROM users WHERE REGEXP_MATCHES(name, '[A-Z]+')";
        let converted = DuckDbDialectConverter::convert(sql).unwrap();
        assert!(converted.contains("LIKE"));
        assert!(converted.contains("WARNING"));
    }

    #[test]
    fn test_list_agg_conversion() {
        let sql = "SELECT LIST_AGG(name) FROM users";
        let converted = DuckDbDialectConverter::convert(sql).unwrap();
        assert!(converted.contains("GROUP_CONCAT"));
    }

    #[test]
    fn test_qualify_conversion_error() {
        let sql = "SELECT * FROM users QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1";
        let result = DuckDbDialectConverter::convert(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_type_mapping() {
        let ddl = "CREATE TABLE test (a HUGEINT, b UTINYINT, c BLOB, d STRUCT(x INT))";
        let converted = SchemaConverter::convert_create_table(ddl).unwrap();
        assert!(converted.contains("BIGINT"));
        assert!(converted.contains("TINYINT"));
        assert!(converted.contains("BINARY"));
        assert!(converted.contains("VARCHAR"));
    }

    #[test]
    fn test_unsupported_features_detection() {
        let ddl = "CREATE TABLE test (data STRUCT(x INT, y INT), tags LIST(VARCHAR))";
        let warnings = SchemaConverter::check_unsupported_features(ddl);
        assert!(warnings.len() >= 2);
        assert!(warnings.iter().any(|w| w.contains("STRUCT")));
        assert!(warnings.iter().any(|w| w.contains("LIST")));
    }

    #[test]
    fn test_migration_planner() {
        let queries = vec![
            "CREATE TABLE users (id HUGEINT, name VARCHAR)".to_string(),
            "SELECT id::INT FROM users".to_string(),
        ];

        let report = MigrationPlanner::plan(queries);
        assert_eq!(report.original_queries.len(), 2);
        assert_eq!(report.converted_queries.len(), 2);

        // Check that HUGEINT was replaced with BIGINT (the comment may mention HUGEINT)
        assert!(report.converted_queries[0].contains("BIGINT"));
        assert!(!report.converted_queries[0].starts_with("CREATE TABLE users (id HUGEINT"));
        assert!(report.converted_queries[1].contains("CAST"));
    }
}
