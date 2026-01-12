//! Natural Language Query Interface
//!
//! This module provides a natural language interface for querying data,
//! allowing users to ask questions in plain English that get translated to SQL.
//!
//! # Features
//!
//! - **Intent Recognition**: Identify query intent (select, aggregate, filter, join)
//! - **Entity Extraction**: Extract table names, columns, values from natural language
//! - **SQL Generation**: Generate valid SQL from parsed natural language
//! - **Context Awareness**: Use schema information for better parsing
//!
//! # Example
//!
//! ```rust,ignore
//! use blaze::nlq::NaturalLanguageProcessor;
//!
//! let processor = NaturalLanguageProcessor::new();
//! let sql = processor.to_sql("Show me all users from California");
//! // Result: "SELECT * FROM users WHERE state = 'California'"
//! ```

mod parser;
mod intent;
mod generator;

pub use parser::{NLParser, ParsedQuery, Token, TokenType};
pub use intent::{QueryIntent, IntentClassifier, AggregateType, FilterOperator};
pub use generator::{SqlGenerator, GeneratorConfig};

use std::collections::HashMap;

use crate::error::Result;
use crate::types::Schema;

/// Natural Language Query Processor.
pub struct NaturalLanguageProcessor {
    /// Parser for tokenizing and analyzing text
    parser: NLParser,
    /// Intent classifier
    classifier: IntentClassifier,
    /// SQL generator
    generator: SqlGenerator,
    /// Schema context
    schema_context: Option<SchemaContext>,
    /// Configuration
    config: NLQConfig,
}

impl NaturalLanguageProcessor {
    /// Create a new NLQ processor.
    pub fn new() -> Self {
        Self {
            parser: NLParser::new(),
            classifier: IntentClassifier::new(),
            generator: SqlGenerator::new(GeneratorConfig::default()),
            schema_context: None,
            config: NLQConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(config: NLQConfig) -> Self {
        Self {
            parser: NLParser::new(),
            classifier: IntentClassifier::new(),
            generator: SqlGenerator::new(config.generator_config.clone()),
            schema_context: None,
            config,
        }
    }

    /// Set schema context for better query understanding.
    pub fn with_schema_context(mut self, context: SchemaContext) -> Self {
        self.schema_context = Some(context);
        self
    }

    /// Convert natural language to SQL.
    pub fn to_sql(&self, query: &str) -> Result<String> {
        // Parse the query
        let parsed = self.parser.parse(query)?;

        // Classify intent
        let intent = self.classifier.classify(&parsed)?;

        // Generate SQL
        let sql = self.generator.generate(&parsed, &intent, self.schema_context.as_ref())?;

        Ok(sql)
    }

    /// Convert natural language to SQL with explanation.
    pub fn to_sql_with_explanation(&self, query: &str) -> Result<QueryResult> {
        let parsed = self.parser.parse(query)?;
        let intent = self.classifier.classify(&parsed)?;
        let sql = self.generator.generate(&parsed, &intent, self.schema_context.as_ref())?;
        let confidence = self.classifier.confidence(&parsed);
        let explanation = self.generate_explanation(&parsed, &intent);

        Ok(QueryResult {
            sql,
            intent,
            confidence,
            explanation,
        })
    }

    /// Generate a human-readable explanation.
    fn generate_explanation(&self, parsed: &ParsedQuery, intent: &QueryIntent) -> String {
        let mut parts = Vec::new();

        match intent {
            QueryIntent::Select { table, columns, .. } => {
                if columns.is_empty() || columns.contains(&"*".to_string()) {
                    parts.push(format!("Selecting all columns from {}", table));
                } else {
                    parts.push(format!(
                        "Selecting {} from {}",
                        columns.join(", "),
                        table
                    ));
                }
            }
            QueryIntent::Aggregate { table, aggregates, group_by, .. } => {
                let agg_desc: Vec<String> = aggregates
                    .iter()
                    .map(|(agg, col)| format!("{:?} of {}", agg, col))
                    .collect();
                parts.push(format!(
                    "Calculating {} from {}",
                    agg_desc.join(", "),
                    table
                ));
                if let Some(group) = group_by {
                    parts.push(format!("grouped by {}", group));
                }
            }
            QueryIntent::Count { table, .. } => {
                parts.push(format!("Counting rows in {}", table));
            }
            QueryIntent::Join { left_table, right_table, .. } => {
                parts.push(format!("Joining {} with {}", left_table, right_table));
            }
        }

        if let Some(filters) = self.extract_filters(parsed) {
            parts.push(format!("where {}", filters));
        }

        parts.join(" ")
    }

    /// Extract filter description from parsed query.
    fn extract_filters(&self, parsed: &ParsedQuery) -> Option<String> {
        let filters: Vec<String> = parsed
            .filters
            .iter()
            .map(|f| format!("{} {} {}", f.column, f.operator.as_str(), f.value))
            .collect();

        if filters.is_empty() {
            None
        } else {
            Some(filters.join(" and "))
        }
    }

    /// Suggest corrections for ambiguous queries.
    pub fn suggest_corrections(&self, query: &str) -> Vec<String> {
        let mut suggestions = Vec::new();

        // Check for common issues
        if !query.to_lowercase().contains("from") {
            if let Some(ref context) = self.schema_context {
                for table in &context.table_names {
                    suggestions.push(format!("{} from {}", query, table));
                }
            }
        }

        suggestions
    }
}

impl Default for NaturalLanguageProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of natural language to SQL conversion.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Generated SQL
    pub sql: String,
    /// Detected intent
    pub intent: QueryIntent,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Human-readable explanation
    pub explanation: String,
}

/// Configuration for NLQ processor.
#[derive(Debug, Clone)]
pub struct NLQConfig {
    /// Enable fuzzy matching for table/column names
    pub fuzzy_matching: bool,
    /// Minimum confidence threshold
    pub min_confidence: f64,
    /// SQL generator configuration
    pub generator_config: GeneratorConfig,
    /// Enable query suggestions
    pub enable_suggestions: bool,
}

impl Default for NLQConfig {
    fn default() -> Self {
        Self {
            fuzzy_matching: true,
            min_confidence: 0.5,
            generator_config: GeneratorConfig::default(),
            enable_suggestions: true,
        }
    }
}

impl NLQConfig {
    /// Create new configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable fuzzy matching.
    pub fn with_fuzzy_matching(mut self, enabled: bool) -> Self {
        self.fuzzy_matching = enabled;
        self
    }

    /// Set minimum confidence threshold.
    pub fn with_min_confidence(mut self, threshold: f64) -> Self {
        self.min_confidence = threshold;
        self
    }
}

/// Schema context for better query understanding.
#[derive(Debug, Clone)]
pub struct SchemaContext {
    /// Available table names
    pub table_names: Vec<String>,
    /// Table schemas
    pub schemas: HashMap<String, Schema>,
    /// Column to table mapping
    pub column_table_map: HashMap<String, String>,
    /// Common aliases
    pub aliases: HashMap<String, String>,
}

impl SchemaContext {
    /// Create a new schema context.
    pub fn new() -> Self {
        Self {
            table_names: Vec::new(),
            schemas: HashMap::new(),
            column_table_map: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    /// Add a table schema.
    pub fn add_table(&mut self, name: &str, schema: Schema) {
        self.table_names.push(name.to_string());

        // Map columns to table
        for field in schema.fields() {
            self.column_table_map
                .insert(field.name().to_string(), name.to_string());
        }

        self.schemas.insert(name.to_string(), schema);
    }

    /// Add an alias.
    pub fn add_alias(&mut self, alias: &str, actual: &str) {
        self.aliases.insert(alias.to_lowercase(), actual.to_string());
    }

    /// Resolve an alias to actual name.
    pub fn resolve_alias(&self, name: &str) -> String {
        self.aliases
            .get(&name.to_lowercase())
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Find table containing a column.
    pub fn find_table_for_column(&self, column: &str) -> Option<&str> {
        self.column_table_map.get(column).map(|s| s.as_str())
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        let resolved = self.resolve_alias(name);
        self.table_names.iter().any(|t| t.eq_ignore_ascii_case(&resolved))
    }

    /// Get column names for a table.
    pub fn columns_for_table(&self, table: &str) -> Vec<String> {
        self.schemas
            .get(table)
            .map(|s| s.fields().iter().map(|f| f.name().to_string()).collect())
            .unwrap_or_default()
    }
}

impl Default for SchemaContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};

    fn create_test_context() -> SchemaContext {
        let mut context = SchemaContext::new();

        let users_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new("state", DataType::Utf8, true),
        ]);

        let orders_schema = Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
        ]);

        context.add_table("users", users_schema);
        context.add_table("orders", orders_schema);
        context.add_alias("customers", "users");
        context.add_alias("purchases", "orders");

        context
    }

    #[test]
    fn test_schema_context() {
        let context = create_test_context();

        assert!(context.has_table("users"));
        assert!(context.has_table("orders"));
        assert!(context.has_table("customers")); // Alias

        assert_eq!(context.resolve_alias("customers"), "users");
        assert_eq!(context.find_table_for_column("name"), Some("users"));
        assert_eq!(context.find_table_for_column("amount"), Some("orders"));
    }

    #[test]
    fn test_nlq_config() {
        let config = NLQConfig::new()
            .with_fuzzy_matching(false)
            .with_min_confidence(0.8);

        assert!(!config.fuzzy_matching);
        assert!((config.min_confidence - 0.8).abs() < 0.01);
    }

    #[test]
    fn test_nlq_processor_creation() {
        let processor = NaturalLanguageProcessor::new();
        let context = create_test_context();
        let processor = processor.with_schema_context(context);

        // Basic test - processor should be created successfully
        assert!(processor.schema_context.is_some());
    }

    #[test]
    fn test_simple_select() {
        let processor = NaturalLanguageProcessor::new()
            .with_schema_context(create_test_context());

        let result = processor.to_sql("show all users");
        assert!(result.is_ok());
        let sql = result.unwrap();
        assert!(sql.to_uppercase().contains("SELECT"));
        assert!(sql.to_uppercase().contains("FROM"));
    }

    #[test]
    fn test_query_with_filter() {
        let processor = NaturalLanguageProcessor::new()
            .with_schema_context(create_test_context());

        let result = processor.to_sql("find users where age > 25");
        assert!(result.is_ok());
    }

    #[test]
    fn test_aggregate_query() {
        let processor = NaturalLanguageProcessor::new()
            .with_schema_context(create_test_context());

        let result = processor.to_sql("count users");
        assert!(result.is_ok());
        let sql = result.unwrap();
        assert!(sql.to_uppercase().contains("COUNT"));
    }

    #[test]
    fn test_query_with_explanation() {
        let processor = NaturalLanguageProcessor::new()
            .with_schema_context(create_test_context());

        let result = processor.to_sql_with_explanation("show all users");
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert!(!query_result.sql.is_empty());
        assert!(!query_result.explanation.is_empty());
    }
}
