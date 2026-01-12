//! SQL Generator
//!
//! This module generates SQL from parsed natural language queries.

use crate::error::{BlazeError, Result};
use super::parser::{ParsedQuery, ParsedFilter};
use super::intent::{QueryIntent, AggregateType, FilterOperator};
use super::SchemaContext;

/// SQL generator configuration.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Use uppercase SQL keywords
    pub uppercase_keywords: bool,
    /// Quote identifiers
    pub quote_identifiers: bool,
    /// Quote character to use
    pub quote_char: char,
    /// Pretty print SQL
    pub pretty_print: bool,
    /// Add semicolon at end
    pub add_semicolon: bool,
    /// Default limit for SELECT queries (0 = no limit)
    pub default_limit: usize,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            uppercase_keywords: true,
            quote_identifiers: false,
            quote_char: '"',
            pretty_print: false,
            add_semicolon: false,
            default_limit: 0,
        }
    }
}

impl GeneratorConfig {
    /// Create a new configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set uppercase keywords.
    pub fn with_uppercase_keywords(mut self, uppercase: bool) -> Self {
        self.uppercase_keywords = uppercase;
        self
    }

    /// Set quote identifiers.
    pub fn with_quote_identifiers(mut self, quote: bool) -> Self {
        self.quote_identifiers = quote;
        self
    }

    /// Set pretty print.
    pub fn with_pretty_print(mut self, pretty: bool) -> Self {
        self.pretty_print = pretty;
        self
    }

    /// Set default limit.
    pub fn with_default_limit(mut self, limit: usize) -> Self {
        self.default_limit = limit;
        self
    }
}

/// SQL generator.
pub struct SqlGenerator {
    /// Configuration
    config: GeneratorConfig,
}

impl SqlGenerator {
    /// Create a new SQL generator.
    pub fn new(config: GeneratorConfig) -> Self {
        Self { config }
    }

    /// Generate SQL from a parsed query and intent.
    pub fn generate(
        &self,
        parsed: &ParsedQuery,
        intent: &QueryIntent,
        schema_context: Option<&SchemaContext>,
    ) -> Result<String> {
        let sql = match intent {
            QueryIntent::Select { table, columns, filters } => {
                self.generate_select(table, columns, filters, schema_context)?
            }
            QueryIntent::Aggregate { table, aggregates, group_by, filters } => {
                self.generate_aggregate(table, aggregates, group_by.as_deref(), filters, schema_context)?
            }
            QueryIntent::Count { table, filters } => {
                self.generate_count(table, filters, schema_context)?
            }
            QueryIntent::Join { left_table, right_table, join_columns } => {
                self.generate_join(left_table, right_table, join_columns, schema_context)?
            }
        };

        Ok(sql)
    }

    /// Generate a SELECT statement.
    fn generate_select(
        &self,
        table: &str,
        columns: &[String],
        filters: &[(String, FilterOperator, String)],
        schema_context: Option<&SchemaContext>,
    ) -> Result<String> {
        let mut parts = Vec::new();

        // SELECT clause
        let select_kw = self.keyword("SELECT");
        let columns_str = if columns.is_empty() || columns.contains(&"*".to_string()) {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| self.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        parts.push(format!("{} {}", select_kw, columns_str));

        // FROM clause
        let from_kw = self.keyword("FROM");
        let resolved_table = self.resolve_table(table, schema_context);
        parts.push(format!("{} {}", from_kw, self.quote_identifier(&resolved_table)));

        // WHERE clause
        if !filters.is_empty() {
            let where_clause = self.generate_where_clause(filters)?;
            parts.push(where_clause);
        }

        // LIMIT clause
        if self.config.default_limit > 0 {
            let limit_kw = self.keyword("LIMIT");
            parts.push(format!("{} {}", limit_kw, self.config.default_limit));
        }

        let separator = if self.config.pretty_print { "\n" } else { " " };
        let mut sql = parts.join(separator);

        if self.config.add_semicolon {
            sql.push(';');
        }

        Ok(sql)
    }

    /// Generate an aggregate statement.
    fn generate_aggregate(
        &self,
        table: &str,
        aggregates: &[(AggregateType, String)],
        group_by: Option<&str>,
        filters: &[(String, FilterOperator, String)],
        schema_context: Option<&SchemaContext>,
    ) -> Result<String> {
        let mut parts = Vec::new();

        // SELECT clause with aggregates
        let select_kw = self.keyword("SELECT");
        let mut select_items: Vec<String> = Vec::new();

        // Add group by column first if present
        if let Some(group_col) = group_by {
            select_items.push(self.quote_identifier(group_col));
        }

        // Add aggregate functions
        for (agg_type, column) in aggregates {
            let agg_str = self.format_aggregate(agg_type, column);
            select_items.push(agg_str);
        }

        parts.push(format!("{} {}", select_kw, select_items.join(", ")));

        // FROM clause
        let from_kw = self.keyword("FROM");
        let resolved_table = self.resolve_table(table, schema_context);
        parts.push(format!("{} {}", from_kw, self.quote_identifier(&resolved_table)));

        // WHERE clause
        if !filters.is_empty() {
            let where_clause = self.generate_where_clause(filters)?;
            parts.push(where_clause);
        }

        // GROUP BY clause
        if let Some(group_col) = group_by {
            let group_kw = self.keyword("GROUP BY");
            parts.push(format!("{} {}", group_kw, self.quote_identifier(group_col)));
        }

        let separator = if self.config.pretty_print { "\n" } else { " " };
        let mut sql = parts.join(separator);

        if self.config.add_semicolon {
            sql.push(';');
        }

        Ok(sql)
    }

    /// Generate a COUNT statement.
    fn generate_count(
        &self,
        table: &str,
        filters: &[(String, FilterOperator, String)],
        schema_context: Option<&SchemaContext>,
    ) -> Result<String> {
        let mut parts = Vec::new();

        // SELECT COUNT(*)
        let select_kw = self.keyword("SELECT");
        let count_fn = self.keyword("COUNT");
        parts.push(format!("{} {}(*)", select_kw, count_fn));

        // FROM clause
        let from_kw = self.keyword("FROM");
        let resolved_table = self.resolve_table(table, schema_context);
        parts.push(format!("{} {}", from_kw, self.quote_identifier(&resolved_table)));

        // WHERE clause
        if !filters.is_empty() {
            let where_clause = self.generate_where_clause(filters)?;
            parts.push(where_clause);
        }

        let separator = if self.config.pretty_print { "\n" } else { " " };
        let mut sql = parts.join(separator);

        if self.config.add_semicolon {
            sql.push(';');
        }

        Ok(sql)
    }

    /// Generate a JOIN statement.
    fn generate_join(
        &self,
        left_table: &str,
        right_table: &str,
        join_columns: &[(String, String)],
        schema_context: Option<&SchemaContext>,
    ) -> Result<String> {
        let mut parts = Vec::new();

        // SELECT *
        let select_kw = self.keyword("SELECT");
        parts.push(format!("{} *", select_kw));

        // FROM left_table
        let from_kw = self.keyword("FROM");
        let resolved_left = self.resolve_table(left_table, schema_context);
        parts.push(format!("{} {}", from_kw, self.quote_identifier(&resolved_left)));

        // JOIN right_table
        let join_kw = self.keyword("JOIN");
        let resolved_right = self.resolve_table(right_table, schema_context);

        if join_columns.is_empty() {
            // Natural join or cross join
            parts.push(format!("{} {}", join_kw, self.quote_identifier(&resolved_right)));
        } else {
            // Join with ON clause
            let on_kw = self.keyword("ON");
            let on_conditions: Vec<String> = join_columns
                .iter()
                .map(|(left_col, right_col)| {
                    format!(
                        "{}.{} = {}.{}",
                        self.quote_identifier(&resolved_left),
                        self.quote_identifier(left_col),
                        self.quote_identifier(&resolved_right),
                        self.quote_identifier(right_col)
                    )
                })
                .collect();

            let and_kw = self.keyword("AND");
            parts.push(format!(
                "{} {} {} {}",
                join_kw,
                self.quote_identifier(&resolved_right),
                on_kw,
                on_conditions.join(&format!(" {} ", and_kw))
            ));
        }

        let separator = if self.config.pretty_print { "\n" } else { " " };
        let mut sql = parts.join(separator);

        if self.config.add_semicolon {
            sql.push(';');
        }

        Ok(sql)
    }

    /// Generate WHERE clause from filters.
    fn generate_where_clause(
        &self,
        filters: &[(String, FilterOperator, String)],
    ) -> Result<String> {
        let where_kw = self.keyword("WHERE");
        let and_kw = self.keyword("AND");

        let conditions: Vec<String> = filters
            .iter()
            .map(|(col, op, val)| self.format_condition(col, op, val))
            .collect();

        Ok(format!("{} {}", where_kw, conditions.join(&format!(" {} ", and_kw))))
    }

    /// Format a single condition.
    fn format_condition(&self, column: &str, op: &FilterOperator, value: &str) -> String {
        let quoted_col = self.quote_identifier(column);
        let op_str = self.format_operator(op);

        match op {
            FilterOperator::IsNull | FilterOperator::IsNotNull => {
                format!("{} {}", quoted_col, op_str)
            }
            FilterOperator::In | FilterOperator::NotIn => {
                // Value should be a comma-separated list
                let values: Vec<&str> = value.split(',').map(|s| s.trim()).collect();
                let formatted_values: Vec<String> = values
                    .iter()
                    .map(|v| self.format_value(v))
                    .collect();
                format!("{} {} ({})", quoted_col, op_str, formatted_values.join(", "))
            }
            FilterOperator::Between => {
                // Value should be "low AND high"
                let between_kw = self.keyword("BETWEEN");
                format!("{} {} {}", quoted_col, between_kw, value)
            }
            FilterOperator::Like | FilterOperator::NotLike => {
                // Add wildcards if not present
                let pattern = if value.contains('%') {
                    self.format_value(value)
                } else {
                    self.format_value(&format!("%{}%", value))
                };
                format!("{} {} {}", quoted_col, op_str, pattern)
            }
            _ => {
                let formatted_value = self.format_value(value);
                format!("{} {} {}", quoted_col, op_str, formatted_value)
            }
        }
    }

    /// Format an operator.
    fn format_operator(&self, op: &FilterOperator) -> String {
        if self.config.uppercase_keywords {
            op.as_str().to_uppercase()
        } else {
            op.as_str().to_lowercase()
        }
    }

    /// Format an aggregate function.
    fn format_aggregate(&self, agg_type: &AggregateType, column: &str) -> String {
        let fn_name = if self.config.uppercase_keywords {
            agg_type.sql_name().to_uppercase()
        } else {
            agg_type.sql_name().to_lowercase()
        };

        match agg_type {
            AggregateType::CountDistinct => {
                // Special case: COUNT(DISTINCT column)
                format!("{}({}))", fn_name, self.quote_identifier(column))
            }
            _ => {
                format!("{}({})", fn_name, self.quote_identifier(column))
            }
        }
    }

    /// Format a value for SQL.
    fn format_value(&self, value: &str) -> String {
        // Check if it's a number
        if value.parse::<f64>().is_ok() {
            value.to_string()
        } else if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
            // Boolean
            if self.config.uppercase_keywords {
                value.to_uppercase()
            } else {
                value.to_lowercase()
            }
        } else if value.eq_ignore_ascii_case("null") {
            self.keyword("NULL")
        } else {
            // String - escape single quotes
            let escaped = value.replace('\'', "''");
            format!("'{}'", escaped)
        }
    }

    /// Quote an identifier if needed.
    fn quote_identifier(&self, identifier: &str) -> String {
        if identifier == "*" {
            return "*".to_string();
        }

        if self.config.quote_identifiers {
            format!("{}{}{}", self.config.quote_char, identifier, self.config.quote_char)
        } else {
            // Check if identifier needs quoting (contains spaces, special chars, etc.)
            let needs_quoting = identifier.contains(' ')
                || identifier.contains('-')
                || identifier.chars().next().map(|c| c.is_numeric()).unwrap_or(false);

            if needs_quoting {
                format!("{}{}{}", self.config.quote_char, identifier, self.config.quote_char)
            } else {
                identifier.to_string()
            }
        }
    }

    /// Convert keyword to proper case.
    fn keyword(&self, keyword: &str) -> String {
        if self.config.uppercase_keywords {
            keyword.to_uppercase()
        } else {
            keyword.to_lowercase()
        }
    }

    /// Resolve table name using schema context.
    fn resolve_table(&self, table: &str, schema_context: Option<&SchemaContext>) -> String {
        if let Some(ctx) = schema_context {
            ctx.resolve_alias(table)
        } else {
            table.to_string()
        }
    }
}

impl Default for SqlGenerator {
    fn default() -> Self {
        Self::new(GeneratorConfig::default())
    }
}

/// SQL template for common query patterns.
#[derive(Debug, Clone)]
pub struct SqlTemplate {
    /// Template name
    pub name: String,
    /// Template pattern with placeholders
    pub pattern: String,
    /// Required parameters
    pub parameters: Vec<String>,
}

impl SqlTemplate {
    /// Create a new template.
    pub fn new(name: impl Into<String>, pattern: impl Into<String>, parameters: Vec<String>) -> Self {
        Self {
            name: name.into(),
            pattern: pattern.into(),
            parameters,
        }
    }

    /// Apply parameters to template.
    pub fn apply(&self, params: &std::collections::HashMap<String, String>) -> Result<String> {
        let mut result = self.pattern.clone();

        for param in &self.parameters {
            let placeholder = format!("{{{}}}", param);
            let value = params
                .get(param)
                .ok_or_else(|| BlazeError::invalid_argument(format!("Missing parameter: {}", param)))?;
            result = result.replace(&placeholder, value);
        }

        Ok(result)
    }
}

/// Common SQL templates.
pub fn common_templates() -> Vec<SqlTemplate> {
    vec![
        SqlTemplate::new(
            "select_all",
            "SELECT * FROM {table}",
            vec!["table".to_string()],
        ),
        SqlTemplate::new(
            "select_columns",
            "SELECT {columns} FROM {table}",
            vec!["columns".to_string(), "table".to_string()],
        ),
        SqlTemplate::new(
            "count_rows",
            "SELECT COUNT(*) FROM {table}",
            vec!["table".to_string()],
        ),
        SqlTemplate::new(
            "count_with_filter",
            "SELECT COUNT(*) FROM {table} WHERE {condition}",
            vec!["table".to_string(), "condition".to_string()],
        ),
        SqlTemplate::new(
            "group_count",
            "SELECT {group_column}, COUNT(*) FROM {table} GROUP BY {group_column}",
            vec!["group_column".to_string(), "table".to_string()],
        ),
        SqlTemplate::new(
            "top_n",
            "SELECT * FROM {table} ORDER BY {order_column} DESC LIMIT {limit}",
            vec!["table".to_string(), "order_column".to_string(), "limit".to_string()],
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_parsed_query(table: &str) -> ParsedQuery {
        ParsedQuery {
            original: String::new(),
            table: table.to_string(),
            columns: vec![],
            filters: vec![],
            aggregates: vec![],
            group_by: None,
            order_by: None,
            limit: None,
            tokens: vec![],
        }
    }

    #[test]
    fn test_generate_simple_select() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Select {
            table: "users".to_string(),
            columns: vec!["*".to_string()],
            filters: vec![],
        };

        let parsed = make_parsed_query("users");

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("SELECT *"));
        assert!(sql.contains("FROM users"));
    }

    #[test]
    fn test_generate_select_with_columns() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Select {
            table: "users".to_string(),
            columns: vec!["name".to_string(), "email".to_string()],
            filters: vec![],
        };

        let mut parsed = make_parsed_query("users");
        parsed.columns = vec!["name".to_string(), "email".to_string()];

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("SELECT name, email"));
        assert!(sql.contains("FROM users"));
    }

    #[test]
    fn test_generate_select_with_filter() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Select {
            table: "users".to_string(),
            columns: vec!["*".to_string()],
            filters: vec![
                ("age".to_string(), FilterOperator::GreaterThan, "25".to_string()),
            ],
        };

        let parsed = make_parsed_query("users");

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("WHERE age > 25"));
    }

    #[test]
    fn test_generate_count() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Count {
            table: "orders".to_string(),
            filters: vec![],
        };

        let parsed = make_parsed_query("orders");

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("SELECT COUNT(*)"));
        assert!(sql.contains("FROM orders"));
    }

    #[test]
    fn test_generate_aggregate() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Aggregate {
            table: "orders".to_string(),
            aggregates: vec![
                (AggregateType::Sum, "amount".to_string()),
            ],
            group_by: Some("customer_id".to_string()),
            filters: vec![],
        };

        let mut parsed = make_parsed_query("orders");
        parsed.group_by = Some("customer_id".to_string());

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("SUM(amount)"));
        assert!(sql.contains("GROUP BY customer_id"));
    }

    #[test]
    fn test_generate_join() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Join {
            left_table: "orders".to_string(),
            right_table: "customers".to_string(),
            join_columns: vec![
                ("customer_id".to_string(), "id".to_string()),
            ],
        };

        let parsed = make_parsed_query("orders");

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("FROM orders"));
        assert!(sql.contains("JOIN customers"));
        assert!(sql.contains("ON orders.customer_id = customers.id"));
    }

    #[test]
    fn test_generator_config() {
        let config = GeneratorConfig::new()
            .with_uppercase_keywords(false)
            .with_quote_identifiers(true)
            .with_pretty_print(true);

        let generator = SqlGenerator::new(config);
        let intent = QueryIntent::Select {
            table: "users".to_string(),
            columns: vec!["name".to_string()],
            filters: vec![],
        };

        let mut parsed = make_parsed_query("users");
        parsed.columns = vec!["name".to_string()];

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("select"));
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("\"users\""));
    }

    #[test]
    fn test_like_filter() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Select {
            table: "users".to_string(),
            columns: vec!["*".to_string()],
            filters: vec![
                ("name".to_string(), FilterOperator::Like, "John".to_string()),
            ],
        };

        let parsed = make_parsed_query("users");

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("LIKE '%John%'"));
    }

    #[test]
    fn test_sql_template() {
        let template = SqlTemplate::new(
            "count_rows",
            "SELECT COUNT(*) FROM {table}",
            vec!["table".to_string()],
        );

        let mut params = std::collections::HashMap::new();
        params.insert("table".to_string(), "users".to_string());

        let sql = template.apply(&params).unwrap();
        assert_eq!(sql, "SELECT COUNT(*) FROM users");
    }

    #[test]
    fn test_string_value_escaping() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Select {
            table: "users".to_string(),
            columns: vec!["*".to_string()],
            filters: vec![
                ("name".to_string(), FilterOperator::Equals, "O'Brien".to_string()),
            ],
        };

        let parsed = make_parsed_query("users");

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("'O''Brien'")); // Single quote should be escaped
    }

    #[test]
    fn test_in_filter() {
        let generator = SqlGenerator::default();
        let intent = QueryIntent::Select {
            table: "orders".to_string(),
            columns: vec!["*".to_string()],
            filters: vec![
                ("status".to_string(), FilterOperator::In, "pending, shipped".to_string()),
            ],
        };

        let parsed = make_parsed_query("orders");

        let sql = generator.generate(&parsed, &intent, None).unwrap();
        assert!(sql.contains("IN ('pending', 'shipped')"));
    }

    #[test]
    fn test_common_templates() {
        let templates = common_templates();
        assert!(!templates.is_empty());

        // Test that all templates have valid names and patterns
        for template in &templates {
            assert!(!template.name.is_empty());
            assert!(!template.pattern.is_empty());
        }
    }
}
