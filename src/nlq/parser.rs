//! Natural Language Parser
//!
//! This module provides tokenization and parsing of natural language queries.

use std::collections::HashSet;

use crate::error::{BlazeError, Result};

/// Natural language query parser.
pub struct NLParser {
    /// Stop words to ignore
    stop_words: HashSet<String>,
    /// SQL keywords to recognize
    sql_keywords: HashSet<String>,
    /// Aggregate keywords
    aggregate_keywords: HashSet<String>,
    /// Comparison operators in natural language
    comparison_words: HashSet<String>,
}

impl NLParser {
    /// Create a new parser.
    pub fn new() -> Self {
        Self {
            stop_words: Self::default_stop_words(),
            sql_keywords: Self::default_sql_keywords(),
            aggregate_keywords: Self::default_aggregate_keywords(),
            comparison_words: Self::default_comparison_words(),
        }
    }

    /// Default stop words.
    fn default_stop_words() -> HashSet<String> {
        vec![
            "a", "an", "the", "of", "to", "and", "or", "in", "on", "at", "for", "is", "are", "was",
            "were", "be", "been", "being", "have", "has", "had", "do", "does", "did", "will",
            "would", "could", "should", "may", "might", "must", "shall", "can", "me", "my", "i",
            "we", "our", "us", "you", "your", "please", "thanks", "thank", "hi", "hello",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    /// Default SQL keywords.
    fn default_sql_keywords() -> HashSet<String> {
        vec![
            "select", "from", "where", "group", "by", "order", "having", "limit", "offset", "join",
            "inner", "left", "right", "outer", "on", "as", "distinct", "all", "and", "or", "not",
            "null", "true", "false", "between", "like", "in", "exists",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    /// Default aggregate keywords.
    fn default_aggregate_keywords() -> HashSet<String> {
        vec![
            "count", "sum", "avg", "average", "min", "minimum", "max", "maximum", "total",
            "number", "amount", "quantity", "how many",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    /// Default comparison words.
    fn default_comparison_words() -> HashSet<String> {
        vec![
            "equals", "equal", "is", "are", "was", "were", "greater", "more", "higher", "above",
            "over", "less", "fewer", "lower", "below", "under", "between", "contains", "like",
            "starts", "ends", "not", "isn't", "aren't", "wasn't", "weren't",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    /// Parse a natural language query.
    pub fn parse(&self, query: &str) -> Result<ParsedQuery> {
        let tokens = self.tokenize(query);
        let normalized = self.normalize_tokens(&tokens);

        let table = self.extract_table(&normalized)?;
        let columns = self.extract_columns(&normalized);
        let filters = self.extract_filters(&normalized);
        let aggregates = self.extract_aggregates(&normalized);
        let order_by = self.extract_order_by(&normalized);
        let limit = self.extract_limit(&normalized);
        let group_by = self.extract_group_by(&normalized);

        Ok(ParsedQuery {
            original: query.to_string(),
            tokens: normalized,
            table,
            columns,
            filters,
            aggregates,
            order_by,
            limit,
            group_by,
        })
    }

    /// Tokenize the query string.
    fn tokenize(&self, query: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut current_word = String::new();
        let mut in_quotes = false;

        for c in query.chars() {
            match c {
                '"' | '\'' => {
                    if in_quotes {
                        tokens.push(Token {
                            text: current_word.clone(),
                            token_type: TokenType::QuotedString,
                        });
                        current_word.clear();
                    }
                    in_quotes = !in_quotes;
                }
                ' ' | '\t' | '\n' if !in_quotes => {
                    if !current_word.is_empty() {
                        let token_type = self.classify_token(&current_word);
                        tokens.push(Token {
                            text: current_word.clone(),
                            token_type,
                        });
                        current_word.clear();
                    }
                }
                ',' | '.' | '!' | '?' if !in_quotes => {
                    if !current_word.is_empty() {
                        let token_type = self.classify_token(&current_word);
                        tokens.push(Token {
                            text: current_word.clone(),
                            token_type,
                        });
                        current_word.clear();
                    }
                    // Skip punctuation
                }
                '>' | '<' | '=' if !in_quotes => {
                    if !current_word.is_empty() {
                        let token_type = self.classify_token(&current_word);
                        tokens.push(Token {
                            text: current_word.clone(),
                            token_type,
                        });
                        current_word.clear();
                    }
                    tokens.push(Token {
                        text: c.to_string(),
                        token_type: TokenType::Operator,
                    });
                }
                _ => {
                    current_word.push(c);
                }
            }
        }

        if !current_word.is_empty() {
            let token_type = self.classify_token(&current_word);
            tokens.push(Token {
                text: current_word,
                token_type,
            });
        }

        tokens
    }

    /// Classify a token.
    fn classify_token(&self, word: &str) -> TokenType {
        let lower = word.to_lowercase();

        if self.sql_keywords.contains(&lower) {
            TokenType::Keyword
        } else if self.aggregate_keywords.contains(&lower) {
            TokenType::Aggregate
        } else if self.comparison_words.contains(&lower) {
            TokenType::Comparison
        } else if self.stop_words.contains(&lower) {
            TokenType::StopWord
        } else if lower.parse::<f64>().is_ok() {
            TokenType::Number
        } else {
            TokenType::Identifier
        }
    }

    /// Normalize tokens (lowercase, remove stop words for analysis).
    fn normalize_tokens(&self, tokens: &[Token]) -> Vec<Token> {
        tokens
            .iter()
            .map(|t| Token {
                text: t.text.to_lowercase(),
                token_type: t.token_type.clone(),
            })
            .collect()
    }

    /// Extract table name from tokens.
    fn extract_table(&self, tokens: &[Token]) -> Result<String> {
        // Look for patterns like "from <table>", "in <table>", "of <table>"
        let prepositions = ["from", "in", "of", "table"];

        for (i, token) in tokens.iter().enumerate() {
            if prepositions.contains(&token.text.as_str()) {
                if let Some(next) = tokens.get(i + 1) {
                    if next.token_type == TokenType::Identifier {
                        return Ok(next.text.clone());
                    }
                }
            }
        }

        // Look for identifier after action words
        let action_words = ["show", "list", "get", "find", "display", "count", "select"];

        for (i, token) in tokens.iter().enumerate() {
            if action_words.contains(&token.text.as_str()) {
                // Skip "all" if present
                let next_idx = if tokens.get(i + 1).map(|t| t.text.as_str()) == Some("all") {
                    i + 2
                } else {
                    i + 1
                };

                if let Some(next) = tokens.get(next_idx) {
                    if next.token_type == TokenType::Identifier {
                        return Ok(next.text.clone());
                    }
                }
            }
        }

        // Default to first identifier
        tokens
            .iter()
            .find(|t| t.token_type == TokenType::Identifier)
            .map(|t| t.text.clone())
            .ok_or_else(|| BlazeError::invalid_argument("Could not identify table name"))
    }

    /// Extract column names from tokens.
    fn extract_columns(&self, tokens: &[Token]) -> Vec<String> {
        let mut columns = Vec::new();

        // Look for patterns like "show <column>, <column>"
        let mut in_column_list = false;

        for (i, token) in tokens.iter().enumerate() {
            if ["show", "select", "get", "display"].contains(&token.text.as_str()) {
                in_column_list = true;
                continue;
            }

            if in_column_list {
                if ["from", "where", "order", "group", "limit"].contains(&token.text.as_str()) {
                    break;
                }

                if token.token_type == TokenType::Identifier && token.text != "all" {
                    // Check if this might be a table name (comes after "from")
                    if i > 0 {
                        let prev = &tokens[i - 1];
                        if prev.text != "from" && prev.text != "in" {
                            columns.push(token.text.clone());
                        }
                    }
                }
            }
        }

        columns
    }

    /// Extract filter conditions from tokens.
    fn extract_filters(&self, tokens: &[Token]) -> Vec<ParsedFilter> {
        let mut filters = Vec::new();
        let mut in_where = false;
        let mut i = 0;

        while i < tokens.len() {
            let token = &tokens[i];

            if token.text == "where" || token.text == "with" || token.text == "having" {
                in_where = true;
                i += 1;
                continue;
            }

            if in_where {
                if ["order", "group", "limit"].contains(&token.text.as_str()) {
                    break;
                }

                // Look for pattern: column operator value
                if token.token_type == TokenType::Identifier {
                    if let Some(filter) = self.parse_filter_at(tokens, i) {
                        filters.push(filter.0);
                        i = filter.1;
                        continue;
                    }
                }
            }

            // Also look for inline filters like "users older than 25"
            if token.token_type == TokenType::Comparison {
                if let Some(filter) = self.parse_inline_filter(tokens, i) {
                    filters.push(filter);
                }
            }

            i += 1;
        }

        filters
    }

    /// Parse a filter at a specific position.
    fn parse_filter_at(&self, tokens: &[Token], start: usize) -> Option<(ParsedFilter, usize)> {
        let column = &tokens[start];

        // Look for operator
        let mut op_idx = start + 1;
        let mut operator = FilterOp::Equals;

        while op_idx < tokens.len() {
            let op_token = &tokens[op_idx];

            if op_token.token_type == TokenType::Operator {
                operator = match op_token.text.as_str() {
                    ">" => FilterOp::GreaterThan,
                    "<" => FilterOp::LessThan,
                    ">=" => FilterOp::GreaterOrEqual,
                    "<=" => FilterOp::LessOrEqual,
                    "=" => FilterOp::Equals,
                    "!=" | "<>" => FilterOp::NotEquals,
                    _ => FilterOp::Equals,
                };
                break;
            }

            if ["is", "equals", "equal", "="].contains(&op_token.text.as_str()) {
                operator = FilterOp::Equals;
                break;
            }

            if ["greater", "more", "above", ">"].contains(&op_token.text.as_str()) {
                operator = FilterOp::GreaterThan;
                break;
            }

            if ["less", "fewer", "below", "<"].contains(&op_token.text.as_str()) {
                operator = FilterOp::LessThan;
                break;
            }

            if op_token.token_type == TokenType::StopWord {
                op_idx += 1;
                continue;
            }

            break;
        }

        // Look for value
        let mut val_idx = op_idx + 1;
        while val_idx < tokens.len() {
            let val_token = &tokens[val_idx];

            if val_token.text == "than" || val_token.text == "to" {
                val_idx += 1;
                continue;
            }

            if val_token.token_type == TokenType::Number
                || val_token.token_type == TokenType::QuotedString
                || val_token.token_type == TokenType::Identifier
            {
                return Some((
                    ParsedFilter {
                        column: column.text.clone(),
                        operator,
                        value: val_token.text.clone(),
                    },
                    val_idx + 1,
                ));
            }

            break;
        }

        None
    }

    /// Parse an inline filter (e.g., "users older than 25").
    fn parse_inline_filter(&self, tokens: &[Token], comp_idx: usize) -> Option<ParsedFilter> {
        let comp_token = &tokens[comp_idx];

        // Look backward for column
        let mut col_idx = comp_idx.saturating_sub(1);
        while col_idx > 0 {
            if tokens[col_idx].token_type == TokenType::Identifier {
                break;
            }
            col_idx -= 1;
        }

        // Look forward for value
        let mut val_idx = comp_idx + 1;
        while val_idx < tokens.len() {
            if tokens[val_idx].text == "than" || tokens[val_idx].text == "to" {
                val_idx += 1;
                continue;
            }
            if tokens[val_idx].token_type == TokenType::Number
                || tokens[val_idx].token_type == TokenType::Identifier
            {
                break;
            }
            val_idx += 1;
        }

        if val_idx < tokens.len() && col_idx < comp_idx {
            let operator = match comp_token.text.as_str() {
                "greater" | "more" | "higher" | "above" | "over" | "older" => FilterOp::GreaterThan,
                "less" | "fewer" | "lower" | "below" | "under" | "younger" => FilterOp::LessThan,
                _ => FilterOp::Equals,
            };

            return Some(ParsedFilter {
                column: tokens[col_idx].text.clone(),
                operator,
                value: tokens[val_idx].text.clone(),
            });
        }

        None
    }

    /// Extract aggregate functions from tokens.
    fn extract_aggregates(&self, tokens: &[Token]) -> Vec<(AggregateType, String)> {
        let mut aggregates = Vec::new();

        for (i, token) in tokens.iter().enumerate() {
            let agg_type = match token.text.as_str() {
                "count" | "number" | "how" => Some(AggregateType::Count),
                "sum" | "total" => Some(AggregateType::Sum),
                "avg" | "average" => Some(AggregateType::Avg),
                "min" | "minimum" | "lowest" => Some(AggregateType::Min),
                "max" | "maximum" | "highest" => Some(AggregateType::Max),
                _ => None,
            };

            if let Some(agg) = agg_type {
                // Look for column
                let col = tokens
                    .iter()
                    .skip(i + 1)
                    .find(|t| t.token_type == TokenType::Identifier)
                    .map(|t| t.text.clone())
                    .unwrap_or_else(|| "*".to_string());

                aggregates.push((agg, col));
            }
        }

        aggregates
    }

    /// Extract ORDER BY clause.
    fn extract_order_by(&self, tokens: &[Token]) -> Option<(String, bool)> {
        for (i, token) in tokens.iter().enumerate() {
            if token.text == "order" || token.text == "sort" {
                // Look for column
                let col = tokens
                    .iter()
                    .skip(i + 1)
                    .find(|t| t.token_type == TokenType::Identifier)
                    .map(|t| t.text.clone());

                // Check for ASC/DESC
                let ascending = !tokens.iter().skip(i + 1).any(|t| {
                    ["desc", "descending", "decreasing", "reverse"].contains(&t.text.as_str())
                });

                if let Some(column) = col {
                    return Some((column, ascending));
                }
            }
        }

        None
    }

    /// Extract LIMIT clause.
    fn extract_limit(&self, tokens: &[Token]) -> Option<usize> {
        for (i, token) in tokens.iter().enumerate() {
            if ["limit", "top", "first", "only"].contains(&token.text.as_str()) {
                // Look for number
                if let Some(num_token) = tokens.get(i + 1) {
                    if let Ok(n) = num_token.text.parse() {
                        return Some(n);
                    }
                }
            }
        }

        None
    }

    /// Extract GROUP BY clause.
    fn extract_group_by(&self, tokens: &[Token]) -> Option<String> {
        for (i, token) in tokens.iter().enumerate() {
            if token.text == "group" || token.text == "by" || token.text == "per" {
                // Look for column
                if let Some(next) = tokens.get(i + 1) {
                    if next.token_type == TokenType::Identifier {
                        return Some(next.text.clone());
                    }
                }
            }
        }

        None
    }
}

impl Default for NLParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parsed natural language query.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Original query text
    pub original: String,
    /// Tokenized query
    pub tokens: Vec<Token>,
    /// Target table
    pub table: String,
    /// Selected columns
    pub columns: Vec<String>,
    /// Filter conditions
    pub filters: Vec<ParsedFilter>,
    /// Aggregate functions
    pub aggregates: Vec<(AggregateType, String)>,
    /// Order by (column, ascending)
    pub order_by: Option<(String, bool)>,
    /// Limit
    pub limit: Option<usize>,
    /// Group by column
    pub group_by: Option<String>,
}

/// A token from parsing.
#[derive(Debug, Clone)]
pub struct Token {
    /// Token text
    pub text: String,
    /// Token type
    pub token_type: TokenType,
}

/// Type of token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenType {
    /// SQL keyword
    Keyword,
    /// Aggregate function word
    Aggregate,
    /// Comparison word
    Comparison,
    /// Operator symbol
    Operator,
    /// Stop word (to be ignored)
    StopWord,
    /// Number literal
    Number,
    /// Quoted string
    QuotedString,
    /// Identifier (table/column name)
    Identifier,
}

/// Parsed filter condition.
#[derive(Debug, Clone)]
pub struct ParsedFilter {
    /// Column name
    pub column: String,
    /// Operator
    pub operator: FilterOp,
    /// Value
    pub value: String,
}

impl ParsedFilter {
    /// Get SQL operator string.
    pub fn sql_operator(&self) -> &'static str {
        self.operator.as_str()
    }
}

/// Filter operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    Like,
    In,
    Between,
    IsNull,
    IsNotNull,
}

impl FilterOp {
    /// Get SQL string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            FilterOp::Equals => "=",
            FilterOp::NotEquals => "!=",
            FilterOp::GreaterThan => ">",
            FilterOp::LessThan => "<",
            FilterOp::GreaterOrEqual => ">=",
            FilterOp::LessOrEqual => "<=",
            FilterOp::Like => "LIKE",
            FilterOp::In => "IN",
            FilterOp::Between => "BETWEEN",
            FilterOp::IsNull => "IS NULL",
            FilterOp::IsNotNull => "IS NOT NULL",
        }
    }
}

/// Aggregate type for parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        let parser = NLParser::new();
        let tokens = parser.tokenize("show all users from table");

        assert!(tokens.len() > 0);
        assert!(tokens.iter().any(|t| t.text == "show"));
        assert!(tokens.iter().any(|t| t.text == "users"));
    }

    #[test]
    fn test_parse_simple_query() {
        let parser = NLParser::new();
        let parsed = parser.parse("show all users").unwrap();

        assert_eq!(parsed.table, "users");
    }

    #[test]
    fn test_parse_with_filter() {
        let parser = NLParser::new();
        let parsed = parser.parse("find users where age > 25").unwrap();

        assert_eq!(parsed.table, "users");
        assert!(!parsed.filters.is_empty());
    }

    #[test]
    fn test_parse_with_limit() {
        let parser = NLParser::new();
        let parsed = parser.parse("show top 10 users").unwrap();

        assert_eq!(parsed.limit, Some(10));
    }

    #[test]
    fn test_parse_aggregate() {
        let parser = NLParser::new();
        let parsed = parser.parse("count users").unwrap();

        assert!(!parsed.aggregates.is_empty());
        assert_eq!(parsed.aggregates[0].0, AggregateType::Count);
    }

    #[test]
    fn test_filter_op_str() {
        assert_eq!(FilterOp::Equals.as_str(), "=");
        assert_eq!(FilterOp::GreaterThan.as_str(), ">");
        assert_eq!(FilterOp::Like.as_str(), "LIKE");
    }
}
