//! Intent Classification
//!
//! This module provides intent classification for natural language queries.

use crate::error::{BlazeError, Result};
use super::parser::{ParsedQuery, ParsedFilter, FilterOp, AggregateType as ParsedAggType};

/// Query intent.
#[derive(Debug, Clone)]
pub enum QueryIntent {
    /// Simple SELECT query
    Select {
        table: String,
        columns: Vec<String>,
        filters: Vec<(String, FilterOperator, String)>,
    },
    /// Aggregate query (COUNT, SUM, etc.)
    Aggregate {
        table: String,
        aggregates: Vec<(AggregateType, String)>,
        group_by: Option<String>,
        filters: Vec<(String, FilterOperator, String)>,
    },
    /// Count query (special case of aggregate)
    Count {
        table: String,
        filters: Vec<(String, FilterOperator, String)>,
    },
    /// Join query
    Join {
        left_table: String,
        right_table: String,
        join_columns: Vec<(String, String)>,
    },
}

/// Aggregate function type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

impl AggregateType {
    /// Get SQL function name.
    pub fn sql_name(&self) -> &'static str {
        match self {
            AggregateType::Count => "COUNT",
            AggregateType::Sum => "SUM",
            AggregateType::Avg => "AVG",
            AggregateType::Min => "MIN",
            AggregateType::Max => "MAX",
            AggregateType::CountDistinct => "COUNT(DISTINCT",
        }
    }
}

impl From<ParsedAggType> for AggregateType {
    fn from(p: ParsedAggType) -> Self {
        match p {
            ParsedAggType::Count => AggregateType::Count,
            ParsedAggType::Sum => AggregateType::Sum,
            ParsedAggType::Avg => AggregateType::Avg,
            ParsedAggType::Min => AggregateType::Min,
            ParsedAggType::Max => AggregateType::Max,
        }
    }
}

/// Filter operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    Like,
    NotLike,
    In,
    NotIn,
    IsNull,
    IsNotNull,
    Between,
}

impl FilterOperator {
    /// Get SQL representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            FilterOperator::Equals => "=",
            FilterOperator::NotEquals => "!=",
            FilterOperator::GreaterThan => ">",
            FilterOperator::LessThan => "<",
            FilterOperator::GreaterOrEqual => ">=",
            FilterOperator::LessOrEqual => "<=",
            FilterOperator::Like => "LIKE",
            FilterOperator::NotLike => "NOT LIKE",
            FilterOperator::In => "IN",
            FilterOperator::NotIn => "NOT IN",
            FilterOperator::IsNull => "IS NULL",
            FilterOperator::IsNotNull => "IS NOT NULL",
            FilterOperator::Between => "BETWEEN",
        }
    }
}

impl From<FilterOp> for FilterOperator {
    fn from(op: FilterOp) -> Self {
        match op {
            FilterOp::Equals => FilterOperator::Equals,
            FilterOp::NotEquals => FilterOperator::NotEquals,
            FilterOp::GreaterThan => FilterOperator::GreaterThan,
            FilterOp::LessThan => FilterOperator::LessThan,
            FilterOp::GreaterOrEqual => FilterOperator::GreaterOrEqual,
            FilterOp::LessOrEqual => FilterOperator::LessOrEqual,
            FilterOp::Like => FilterOperator::Like,
            FilterOp::In => FilterOperator::In,
            FilterOp::Between => FilterOperator::Between,
            FilterOp::IsNull => FilterOperator::IsNull,
            FilterOp::IsNotNull => FilterOperator::IsNotNull,
        }
    }
}

/// Intent classifier.
pub struct IntentClassifier {
    /// Classification weights
    weights: ClassificationWeights,
}

/// Weights for intent classification.
#[derive(Debug, Clone)]
struct ClassificationWeights {
    /// Weight for aggregate keywords
    aggregate_weight: f64,
    /// Weight for join keywords
    join_weight: f64,
    /// Weight for count keywords
    count_weight: f64,
}

impl Default for ClassificationWeights {
    fn default() -> Self {
        Self {
            aggregate_weight: 1.0,
            join_weight: 1.2,
            count_weight: 0.8,
        }
    }
}

impl IntentClassifier {
    /// Create a new intent classifier.
    pub fn new() -> Self {
        Self {
            weights: ClassificationWeights::default(),
        }
    }

    /// Classify the intent of a parsed query.
    pub fn classify(&self, parsed: &ParsedQuery) -> Result<QueryIntent> {
        let filters = self.convert_filters(&parsed.filters);

        // Check for aggregate intent
        if !parsed.aggregates.is_empty() {
            // Special case for simple count
            if parsed.aggregates.len() == 1
                && parsed.aggregates[0].0 == ParsedAggType::Count
                && parsed.group_by.is_none()
            {
                return Ok(QueryIntent::Count {
                    table: parsed.table.clone(),
                    filters,
                });
            }

            let aggregates: Vec<(AggregateType, String)> = parsed
                .aggregates
                .iter()
                .map(|(a, c)| ((*a).into(), c.clone()))
                .collect();

            return Ok(QueryIntent::Aggregate {
                table: parsed.table.clone(),
                aggregates,
                group_by: parsed.group_by.clone(),
                filters,
            });
        }

        // Check for join intent (look for join keywords in tokens)
        let has_join = parsed.tokens.iter().any(|t| {
            ["join", "with", "and", "combine", "merge"].contains(&t.text.as_str())
        });

        if has_join {
            // Try to extract second table
            let tables: Vec<&str> = parsed
                .tokens
                .iter()
                .filter(|t| t.token_type == super::parser::TokenType::Identifier)
                .map(|t| t.text.as_str())
                .collect();

            if tables.len() >= 2 {
                return Ok(QueryIntent::Join {
                    left_table: tables[0].to_string(),
                    right_table: tables[1].to_string(),
                    join_columns: vec![],
                });
            }
        }

        // Default to simple select
        Ok(QueryIntent::Select {
            table: parsed.table.clone(),
            columns: if parsed.columns.is_empty() {
                vec!["*".to_string()]
            } else {
                parsed.columns.clone()
            },
            filters,
        })
    }

    /// Convert parsed filters to intent filters.
    fn convert_filters(
        &self,
        parsed_filters: &[ParsedFilter],
    ) -> Vec<(String, FilterOperator, String)> {
        parsed_filters
            .iter()
            .map(|f| (f.column.clone(), f.operator.into(), f.value.clone()))
            .collect()
    }

    /// Calculate confidence score for classification.
    pub fn confidence(&self, parsed: &ParsedQuery) -> f64 {
        let mut score: f64 = 0.5; // Base score

        // Having a clear table increases confidence
        if !parsed.table.is_empty() {
            score += 0.2;
        }

        // Having aggregates increases confidence for aggregate intent
        if !parsed.aggregates.is_empty() {
            score += 0.15;
        }

        // Having clear filters increases confidence
        if !parsed.filters.is_empty() {
            score += 0.1;
        }

        // Having more tokens generally means more context
        if parsed.tokens.len() > 3 {
            score += 0.05;
        }

        score.min(1.0)
    }

    /// Get the dominant intent type.
    pub fn dominant_intent(&self, parsed: &ParsedQuery) -> &'static str {
        if !parsed.aggregates.is_empty() {
            if parsed.aggregates.iter().all(|(a, _)| *a == ParsedAggType::Count) {
                "count"
            } else {
                "aggregate"
            }
        } else {
            "select"
        }
    }
}

impl Default for IntentClassifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Intent validation result.
#[derive(Debug, Clone)]
pub struct IntentValidation {
    /// Whether the intent is valid
    pub valid: bool,
    /// Validation messages
    pub messages: Vec<String>,
    /// Suggestions for improvement
    pub suggestions: Vec<String>,
}

impl IntentValidation {
    /// Create a valid result.
    pub fn valid() -> Self {
        Self {
            valid: true,
            messages: vec![],
            suggestions: vec![],
        }
    }

    /// Create an invalid result.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            valid: false,
            messages: vec![message.into()],
            suggestions: vec![],
        }
    }

    /// Add a suggestion.
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestions.push(suggestion.into());
        self
    }
}

/// Validate an intent.
pub fn validate_intent(intent: &QueryIntent) -> IntentValidation {
    match intent {
        QueryIntent::Select { table, columns, .. } => {
            if table.is_empty() {
                return IntentValidation::invalid("No table specified")
                    .with_suggestion("Try specifying a table name");
            }
            IntentValidation::valid()
        }
        QueryIntent::Aggregate { table, aggregates, .. } => {
            if table.is_empty() {
                return IntentValidation::invalid("No table specified");
            }
            if aggregates.is_empty() {
                return IntentValidation::invalid("No aggregates specified");
            }
            IntentValidation::valid()
        }
        QueryIntent::Count { table, .. } => {
            if table.is_empty() {
                return IntentValidation::invalid("No table specified");
            }
            IntentValidation::valid()
        }
        QueryIntent::Join { left_table, right_table, .. } => {
            if left_table.is_empty() || right_table.is_empty() {
                return IntentValidation::invalid("Both tables must be specified for join");
            }
            IntentValidation::valid()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::parser::NLParser;

    #[test]
    fn test_classify_select() {
        let parser = NLParser::new();
        let classifier = IntentClassifier::new();

        let parsed = parser.parse("show all users").unwrap();
        let intent = classifier.classify(&parsed).unwrap();

        match intent {
            QueryIntent::Select { table, .. } => {
                assert_eq!(table, "users");
            }
            _ => panic!("Expected Select intent"),
        }
    }

    #[test]
    fn test_classify_count() {
        let parser = NLParser::new();
        let classifier = IntentClassifier::new();

        let parsed = parser.parse("count users").unwrap();
        let intent = classifier.classify(&parsed).unwrap();

        match intent {
            QueryIntent::Count { table, .. } => {
                assert_eq!(table, "users");
            }
            _ => panic!("Expected Count intent"),
        }
    }

    #[test]
    fn test_classify_aggregate() {
        let parser = NLParser::new();
        let classifier = IntentClassifier::new();

        let parsed = parser.parse("sum amount from orders group by customer").unwrap();
        let intent = classifier.classify(&parsed).unwrap();

        match intent {
            QueryIntent::Aggregate { table, aggregates, group_by, .. } => {
                assert_eq!(table, "orders");
                assert!(!aggregates.is_empty());
            }
            _ => panic!("Expected Aggregate intent"),
        }
    }

    #[test]
    fn test_confidence() {
        let parser = NLParser::new();
        let classifier = IntentClassifier::new();

        let parsed = parser.parse("show all users where age > 25").unwrap();
        let confidence = classifier.confidence(&parsed);

        assert!(confidence > 0.5);
        assert!(confidence <= 1.0);
    }

    #[test]
    fn test_aggregate_type() {
        assert_eq!(AggregateType::Count.sql_name(), "COUNT");
        assert_eq!(AggregateType::Sum.sql_name(), "SUM");
        assert_eq!(AggregateType::Avg.sql_name(), "AVG");
    }

    #[test]
    fn test_filter_operator() {
        assert_eq!(FilterOperator::Equals.as_str(), "=");
        assert_eq!(FilterOperator::GreaterThan.as_str(), ">");
        assert_eq!(FilterOperator::Like.as_str(), "LIKE");
    }

    #[test]
    fn test_validate_intent() {
        let valid_intent = QueryIntent::Select {
            table: "users".to_string(),
            columns: vec!["*".to_string()],
            filters: vec![],
        };

        let validation = validate_intent(&valid_intent);
        assert!(validation.valid);

        let invalid_intent = QueryIntent::Select {
            table: "".to_string(),
            columns: vec![],
            filters: vec![],
        };

        let validation = validate_intent(&invalid_intent);
        assert!(!validation.valid);
    }
}
