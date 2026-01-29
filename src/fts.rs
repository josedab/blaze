//! Full-Text Search Engine
//!
//! Provides inverted index-based full-text search with BM25 ranking,
//! boolean query support, tokenization, and fuzzy matching.
//!
//! # Example
//!
//! ```rust
//! use blaze::fts::{FtsIndex, Tokenizer, FtsQuery};
//!
//! let mut index = FtsIndex::new();
//! let tokenizer = Tokenizer::default();
//!
//! index.add_document(0, "The quick brown fox", &tokenizer);
//! index.add_document(1, "A lazy brown dog", &tokenizer);
//!
//! let query = FtsQuery::Term("brown".to_string());
//! let results = index.search(&query, &tokenizer, 10);
//! assert_eq!(results.len(), 2);
//! ```

use std::collections::HashMap;

use crate::error::{BlazeError, Result};

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

/// Text tokenizer with configurable processing pipeline.
#[derive(Debug, Clone)]
pub struct Tokenizer {
    pub lowercase: bool,
    pub stop_words: Vec<String>,
    pub min_token_length: usize,
    pub max_token_length: usize,
    pub stemmer: Option<StemmerLanguage>,
}

/// Supported stemmer languages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StemmerLanguage {
    English,
    Spanish,
    German,
    French,
}

impl Default for Tokenizer {
    fn default() -> Self {
        Self {
            lowercase: true,
            stop_words: default_stop_words(),
            min_token_length: 1,
            max_token_length: 256,
            stemmer: Some(StemmerLanguage::English),
        }
    }
}

impl Tokenizer {
    /// Create a tokenizer with no processing (raw split on whitespace).
    pub fn raw() -> Self {
        Self {
            lowercase: false,
            stop_words: Vec::new(),
            min_token_length: 1,
            max_token_length: 256,
            stemmer: None,
        }
    }

    /// Tokenize text into a list of processed tokens.
    pub fn tokenize(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();

        for word in text.split(|c: char| !c.is_alphanumeric() && c != '\'') {
            if word.is_empty() {
                continue;
            }

            let mut token = if self.lowercase {
                word.to_lowercase()
            } else {
                word.to_string()
            };

            // Remove trailing apostrophe-s
            if token.ends_with("'s") {
                token.truncate(token.len() - 2);
            }

            if token.len() < self.min_token_length || token.len() > self.max_token_length {
                continue;
            }

            if self.stop_words.contains(&token) {
                continue;
            }

            if let Some(lang) = self.stemmer {
                token = stem(&token, lang);
            }

            tokens.push(token);
        }

        tokens
    }
}

/// Simple English stemmer (Porter-style suffix stripping).
fn stem(word: &str, lang: StemmerLanguage) -> String {
    match lang {
        StemmerLanguage::English => stem_english(word),
        _ => word.to_string(), // Other languages: identity for now
    }
}

fn stem_english(word: &str) -> String {
    let mut s = word.to_string();
    // Simple suffix-stripping rules
    for &(suffix, replacement) in &[
        ("izing", "ize"),
        ("ating", "ate"),
        ("ional", "ion"),
        ("iness", "y"),
        ("ness", ""),
        ("ment", ""),
        ("tion", "t"),
        ("sion", "s"),
        ("ful", ""),
        ("ing", ""),
        ("ous", ""),
        ("ive", ""),
        ("ies", "y"),
        ("ly", ""),
        ("ed", ""),
        ("er", ""),
        ("es", ""),
        ("s", ""),
    ] {
        if s.len() > suffix.len() + 2 && s.ends_with(suffix) {
            s.truncate(s.len() - suffix.len());
            s.push_str(replacement);
            return s;
        }
    }
    s
}

fn default_stop_words() -> Vec<String> {
    vec![
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
        "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

// ---------------------------------------------------------------------------
// Inverted Index
// ---------------------------------------------------------------------------

/// A posting list entry for one document.
#[derive(Debug, Clone)]
pub struct Posting {
    pub doc_id: u64,
    pub term_frequency: u32,
    pub positions: Vec<u32>,
}

/// Full-text search inverted index.
#[derive(Debug)]
pub struct FtsIndex {
    /// Term → posting list
    postings: HashMap<String, Vec<Posting>>,
    /// Document metadata
    doc_lengths: HashMap<u64, u32>,
    /// Total number of documents
    doc_count: u64,
    /// Average document length
    avg_doc_length: f64,
    /// Total tokens across all documents
    total_tokens: u64,
    /// Name of the index
    name: String,
}

impl Default for FtsIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FtsIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_lengths: HashMap::new(),
            doc_count: 0,
            avg_doc_length: 0.0,
            total_tokens: 0,
            name: String::new(),
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Add a document to the index.
    pub fn add_document(&mut self, doc_id: u64, text: &str, tokenizer: &Tokenizer) {
        let tokens = tokenizer.tokenize(text);
        let doc_len = tokens.len() as u32;

        // Build term frequency and position maps
        let mut term_positions: HashMap<String, Vec<u32>> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            term_positions
                .entry(token.clone())
                .or_default()
                .push(pos as u32);
        }

        // Add to postings
        for (term, positions) in term_positions {
            let tf = positions.len() as u32;
            let posting = Posting {
                doc_id,
                term_frequency: tf,
                positions,
            };
            self.postings.entry(term).or_default().push(posting);
        }

        // Update document metadata
        self.doc_lengths.insert(doc_id, doc_len);
        self.total_tokens += doc_len as u64;
        self.doc_count += 1;
        self.avg_doc_length = self.total_tokens as f64 / self.doc_count as f64;
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, doc_id: u64) {
        if let Some(len) = self.doc_lengths.remove(&doc_id) {
            self.total_tokens -= len as u64;
            self.doc_count -= 1;
            self.avg_doc_length = if self.doc_count > 0 {
                self.total_tokens as f64 / self.doc_count as f64
            } else {
                0.0
            };

            // Remove from all posting lists
            for postings in self.postings.values_mut() {
                postings.retain(|p| p.doc_id != doc_id);
            }
            // Clean up empty posting lists
            self.postings.retain(|_, v| !v.is_empty());
        }
    }

    /// Search the index with a query and return ranked results.
    pub fn search(
        &self,
        query: &FtsQuery,
        tokenizer: &Tokenizer,
        limit: usize,
    ) -> Vec<SearchResult> {
        let mut scores: HashMap<u64, f64> = HashMap::new();

        match query {
            FtsQuery::Term(term) => {
                let tokens = tokenizer.tokenize(term);
                for token in &tokens {
                    self.score_term(token, &mut scores);
                }
            }
            FtsQuery::And(terms) => {
                // All terms must be present
                let mut doc_sets: Vec<std::collections::HashSet<u64>> = Vec::new();
                for term in terms {
                    let tokens = tokenizer.tokenize(term);
                    let mut docs = std::collections::HashSet::new();
                    for token in &tokens {
                        if let Some(postings) = self.postings.get(token) {
                            for p in postings {
                                docs.insert(p.doc_id);
                            }
                        }
                    }
                    doc_sets.push(docs);
                }

                if let Some(intersection) = doc_sets
                    .into_iter()
                    .reduce(|a, b| a.intersection(&b).cloned().collect())
                {
                    for term in terms {
                        let tokens = tokenizer.tokenize(term);
                        for token in &tokens {
                            self.score_term_for_docs(token, &intersection, &mut scores);
                        }
                    }
                }
            }
            FtsQuery::Or(terms) => {
                for term in terms {
                    let tokens = tokenizer.tokenize(term);
                    for token in &tokens {
                        self.score_term(token, &mut scores);
                    }
                }
            }
            FtsQuery::Not(include, exclude) => {
                let include_tokens = tokenizer.tokenize(include);
                for token in &include_tokens {
                    self.score_term(token, &mut scores);
                }
                let exclude_tokens = tokenizer.tokenize(exclude);
                for token in &exclude_tokens {
                    if let Some(postings) = self.postings.get(token) {
                        for p in postings {
                            scores.remove(&p.doc_id);
                        }
                    }
                }
            }
            FtsQuery::Phrase(phrase) => {
                let tokens = tokenizer.tokenize(phrase);
                if tokens.is_empty() {
                    return Vec::new();
                }
                let matching_docs = self.find_phrase_matches(&tokens);
                for doc_id in &matching_docs {
                    // Score phrase matches higher
                    let doc_len = *self.doc_lengths.get(doc_id).unwrap_or(&1) as f64;
                    let score = (tokens.len() as f64) * (1.0 + 1.0 / doc_len);
                    *scores.entry(*doc_id).or_insert(0.0) += score;
                }
            }
            FtsQuery::Fuzzy(term, max_distance) => {
                let tokens = tokenizer.tokenize(term);
                for token in &tokens {
                    for (indexed_term, postings) in &self.postings {
                        if levenshtein(token, indexed_term) <= *max_distance as usize {
                            for p in postings {
                                let bm25 = self.bm25_score(
                                    p.term_frequency,
                                    postings.len() as u64,
                                    *self.doc_lengths.get(&p.doc_id).unwrap_or(&1),
                                );
                                *scores.entry(p.doc_id).or_insert(0.0) += bm25;
                            }
                        }
                    }
                }
            }
        }

        let mut results: Vec<SearchResult> = scores
            .into_iter()
            .map(|(doc_id, score)| SearchResult { doc_id, score })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        results
    }

    /// Return index statistics.
    pub fn stats(&self) -> FtsIndexStats {
        FtsIndexStats {
            doc_count: self.doc_count,
            unique_terms: self.postings.len() as u64,
            total_tokens: self.total_tokens,
            avg_doc_length: self.avg_doc_length,
        }
    }

    // -- internal scoring --

    fn score_term(&self, token: &str, scores: &mut HashMap<u64, f64>) {
        if let Some(postings) = self.postings.get(token) {
            let df = postings.len() as u64;
            for p in postings {
                let doc_len = *self.doc_lengths.get(&p.doc_id).unwrap_or(&1);
                let bm25 = self.bm25_score(p.term_frequency, df, doc_len);
                *scores.entry(p.doc_id).or_insert(0.0) += bm25;
            }
        }
    }

    fn score_term_for_docs(
        &self,
        token: &str,
        doc_filter: &std::collections::HashSet<u64>,
        scores: &mut HashMap<u64, f64>,
    ) {
        if let Some(postings) = self.postings.get(token) {
            let df = postings.len() as u64;
            for p in postings {
                if doc_filter.contains(&p.doc_id) {
                    let doc_len = *self.doc_lengths.get(&p.doc_id).unwrap_or(&1);
                    let bm25 = self.bm25_score(p.term_frequency, df, doc_len);
                    *scores.entry(p.doc_id).or_insert(0.0) += bm25;
                }
            }
        }
    }

    /// BM25 scoring formula.
    /// k1 = 1.2, b = 0.75 (standard parameters)
    fn bm25_score(&self, tf: u32, df: u64, doc_len: u32) -> f64 {
        let k1 = 1.2;
        let b = 0.75;
        let n = self.doc_count as f64;
        let df = df as f64;
        let tf = tf as f64;
        let dl = doc_len as f64;
        let avgdl = self.avg_doc_length;

        // IDF component: log((N - df + 0.5) / (df + 0.5) + 1)
        let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

        // TF component with length normalization
        let tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avgdl.max(1.0)));

        idf * tf_norm
    }

    fn find_phrase_matches(&self, tokens: &[String]) -> Vec<u64> {
        if tokens.is_empty() {
            return Vec::new();
        }

        // Get postings for the first token
        let first_postings = match self.postings.get(&tokens[0]) {
            Some(p) => p,
            None => return Vec::new(),
        };

        let mut matching_docs = Vec::new();

        for posting in first_postings {
            let doc_id = posting.doc_id;
            // Check if all subsequent tokens appear at consecutive positions
            'position: for &start_pos in &posting.positions {
                let mut found = true;
                for (offset, token) in tokens.iter().enumerate().skip(1) {
                    let expected_pos = start_pos + offset as u32;
                    if let Some(next_postings) = self.postings.get(token) {
                        let has_position = next_postings
                            .iter()
                            .any(|p| p.doc_id == doc_id && p.positions.contains(&expected_pos));
                        if !has_position {
                            found = false;
                            break;
                        }
                    } else {
                        found = false;
                        break;
                    }
                }
                if found {
                    matching_docs.push(doc_id);
                    break 'position;
                }
            }
        }

        matching_docs
    }
}

// ---------------------------------------------------------------------------
// Query Types
// ---------------------------------------------------------------------------

/// Full-text search query types.
#[derive(Debug, Clone)]
pub enum FtsQuery {
    /// Single term search.
    Term(String),
    /// All terms must match (AND).
    And(Vec<String>),
    /// Any term can match (OR).
    Or(Vec<String>),
    /// First term must match, second must NOT match.
    Not(String, String),
    /// Exact phrase match (terms must appear consecutively).
    Phrase(String),
    /// Fuzzy term match with maximum edit distance.
    Fuzzy(String, u32),
}

impl FtsQuery {
    /// Parse a simple query string into an FtsQuery.
    /// Supports: `term`, `term1 AND term2`, `term1 OR term2`,
    /// `"exact phrase"`, `term~2` (fuzzy).
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(BlazeError::invalid_argument("Empty search query"));
        }

        // Phrase query: "..."
        if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() > 2 {
            return Ok(FtsQuery::Phrase(trimmed[1..trimmed.len() - 1].to_string()));
        }

        // Fuzzy query: term~N
        if let Some(tilde_pos) = trimmed.rfind('~') {
            let term = &trimmed[..tilde_pos];
            let dist = trimmed[tilde_pos + 1..].parse::<u32>().unwrap_or(1);
            if !term.is_empty() {
                return Ok(FtsQuery::Fuzzy(term.to_string(), dist));
            }
        }

        // AND query
        if trimmed.contains(" AND ") {
            let parts: Vec<String> = trimmed
                .split(" AND ")
                .map(|s| s.trim().to_string())
                .collect();
            return Ok(FtsQuery::And(parts));
        }

        // NOT query
        if trimmed.contains(" NOT ") {
            let parts: Vec<&str> = trimmed.splitn(2, " NOT ").collect();
            if parts.len() == 2 {
                return Ok(FtsQuery::Not(
                    parts[0].trim().to_string(),
                    parts[1].trim().to_string(),
                ));
            }
        }

        // OR query
        if trimmed.contains(" OR ") {
            let parts: Vec<String> = trimmed
                .split(" OR ")
                .map(|s| s.trim().to_string())
                .collect();
            return Ok(FtsQuery::Or(parts));
        }

        // Simple term
        Ok(FtsQuery::Term(trimmed.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Search Results
// ---------------------------------------------------------------------------

/// A single search result with document ID and relevance score.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub doc_id: u64,
    pub score: f64,
}

/// Index statistics.
#[derive(Debug, Clone)]
pub struct FtsIndexStats {
    pub doc_count: u64,
    pub unique_terms: u64,
    pub total_tokens: u64,
    pub avg_doc_length: f64,
}

// ---------------------------------------------------------------------------
// Snippet / Highlight
// ---------------------------------------------------------------------------

/// Generate a highlighted snippet from text matching query terms.
pub fn highlight_snippet(
    text: &str,
    query_terms: &[String],
    open_tag: &str,
    close_tag: &str,
    max_length: usize,
) -> String {
    let lower = text.to_lowercase();
    let mut result = String::new();
    let mut found_first = false;
    let mut start_offset = 0;

    // Find the first occurrence of any query term
    for term in query_terms {
        if let Some(pos) = lower.find(&term.to_lowercase()) {
            if !found_first || pos < start_offset {
                start_offset = pos.saturating_sub(40);
                found_first = true;
            }
        }
    }

    // Build snippet around the match
    let snippet_text = if text.len() > max_length {
        let end = (start_offset + max_length).min(text.len());
        &text[start_offset..end]
    } else {
        text
    };

    let snippet_lower = snippet_text.to_lowercase();
    let mut last_end = 0;

    for term in query_terms {
        let term_lower = term.to_lowercase();
        let mut search_from = 0;
        while let Some(pos) = snippet_lower[search_from..].find(&term_lower) {
            let abs_pos = search_from + pos;
            if abs_pos >= last_end {
                result.push_str(&snippet_text[last_end..abs_pos]);
                result.push_str(open_tag);
                result.push_str(&snippet_text[abs_pos..abs_pos + term_lower.len()]);
                result.push_str(close_tag);
                last_end = abs_pos + term_lower.len();
            }
            search_from = abs_pos + 1;
        }
    }

    if last_end < snippet_text.len() {
        result.push_str(&snippet_text[last_end..]);
    }

    if result.is_empty() {
        snippet_text[..max_length.min(snippet_text.len())].to_string()
    } else {
        result
    }
}

// ---------------------------------------------------------------------------
// Levenshtein Distance (for fuzzy matching)
// ---------------------------------------------------------------------------

/// Compute the Levenshtein edit distance between two strings.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();

    let mut prev: Vec<usize> = (0..=b_len).collect();
    let mut curr = vec![0; b_len + 1];

    for i in 1..=a_len {
        curr[0] = i;
        for j in 1..=b_len {
            let cost = if a_bytes[i - 1] == b_bytes[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[b_len]
}

// ---------------------------------------------------------------------------
// Index Manager (multi-index support)
// ---------------------------------------------------------------------------

/// Manages multiple FTS indexes across tables.
#[derive(Debug)]
pub struct FtsIndexManager {
    indexes: HashMap<String, FtsIndex>,
}

impl Default for FtsIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FtsIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    /// Create a new index for a table column.
    pub fn create_index(&mut self, name: &str) -> &mut FtsIndex {
        self.indexes
            .entry(name.to_string())
            .or_insert_with(|| FtsIndex::new().with_name(name))
    }

    /// Get an existing index.
    pub fn get_index(&self, name: &str) -> Option<&FtsIndex> {
        self.indexes.get(name)
    }

    /// Get a mutable reference to an existing index.
    pub fn get_index_mut(&mut self, name: &str) -> Option<&mut FtsIndex> {
        self.indexes.get_mut(name)
    }

    /// Drop an index.
    pub fn drop_index(&mut self, name: &str) -> bool {
        self.indexes.remove(name).is_some()
    }

    /// List all index names.
    pub fn list_indexes(&self) -> Vec<&str> {
        self.indexes.keys().map(|s| s.as_str()).collect()
    }

    /// Total number of indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> FtsIndex {
        let mut index = FtsIndex::new();
        let tokenizer = Tokenizer::default();

        index.add_document(0, "The quick brown fox jumps over the lazy dog", &tokenizer);
        index.add_document(1, "A fast brown cat sits on the mat", &tokenizer);
        index.add_document(2, "The fox is quick and clever", &tokenizer);
        index.add_document(3, "Lazy dogs sleep all day long", &tokenizer);
        index.add_document(4, "Brown bears live in the forest", &tokenizer);
        index
    }

    #[test]
    fn test_tokenizer_basic() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("The Quick Brown FOX");
        assert!(!tokens.contains(&"the".to_string())); // stop word
        assert!(tokens
            .iter()
            .any(|t| t.starts_with("quick") || t == "quick"));
    }

    #[test]
    fn test_tokenizer_raw() {
        let tokenizer = Tokenizer::raw();
        let tokens = tokenizer.tokenize("Hello World");
        assert_eq!(tokens, vec!["Hello", "World"]);
    }

    #[test]
    fn test_index_add_and_stats() {
        let index = create_test_index();
        let stats = index.stats();
        assert_eq!(stats.doc_count, 5);
        assert!(stats.unique_terms > 0);
        assert!(stats.avg_doc_length > 0.0);
    }

    #[test]
    fn test_single_term_search() {
        let index = create_test_index();
        let tokenizer = Tokenizer::default();
        let query = FtsQuery::Term("brown".to_string());
        let results = index.search(&query, &tokenizer, 10);
        // "brown" appears in docs 0, 1, 4
        assert!(results.len() >= 2);
        assert!(results[0].score > 0.0);
    }

    #[test]
    fn test_and_query() {
        let index = create_test_index();
        let tokenizer = Tokenizer::default();
        let query = FtsQuery::And(vec!["brown".into(), "fox".into()]);
        let results = index.search(&query, &tokenizer, 10);
        // Only doc 0 has both "brown" and "fox"
        assert!(!results.is_empty());
        assert!(results.iter().any(|r| r.doc_id == 0));
    }

    #[test]
    fn test_or_query() {
        let index = create_test_index();
        let tokenizer = Tokenizer::default();
        let query = FtsQuery::Or(vec!["fox".into(), "cat".into()]);
        let results = index.search(&query, &tokenizer, 10);
        // fox in docs 0, 2; cat in doc 1
        assert!(results.len() >= 2);
    }

    #[test]
    fn test_not_query() {
        let index = create_test_index();
        let tokenizer = Tokenizer::default();
        let query = FtsQuery::Not("brown".into(), "fox".into());
        let results = index.search(&query, &tokenizer, 10);
        // brown in 0,1,4; exclude fox (0,2) → should have 1 and 4
        assert!(!results.iter().any(|r| r.doc_id == 0));
    }

    #[test]
    fn test_fuzzy_search() {
        let index = create_test_index();
        let tokenizer = Tokenizer::default();
        let query = FtsQuery::Fuzzy("quik".into(), 2); // fuzzy match for "quick"
        let results = index.search(&query, &tokenizer, 10);
        assert!(!results.is_empty());
    }

    #[test]
    fn test_phrase_search() {
        let mut index = FtsIndex::new();
        let tokenizer = Tokenizer::raw();
        index.add_document(0, "the quick brown fox", &tokenizer);
        index.add_document(1, "quick brown", &tokenizer);
        index.add_document(2, "brown quick", &tokenizer);

        let query = FtsQuery::Phrase("quick brown".into());
        let results = index.search(&query, &tokenizer, 10);
        // "quick brown" as phrase should match docs 0 and 1
        let doc_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
        assert!(doc_ids.contains(&0));
        assert!(doc_ids.contains(&1));
        assert!(!doc_ids.contains(&2)); // "brown quick" is reversed
    }

    #[test]
    fn test_remove_document() {
        let mut index = create_test_index();
        let stats_before = index.stats();

        index.remove_document(0);
        let stats_after = index.stats();

        assert_eq!(stats_after.doc_count, stats_before.doc_count - 1);
    }

    #[test]
    fn test_query_parse_term() {
        let q = FtsQuery::parse("hello").unwrap();
        assert!(matches!(q, FtsQuery::Term(_)));
    }

    #[test]
    fn test_query_parse_and() {
        let q = FtsQuery::parse("hello AND world").unwrap();
        assert!(matches!(q, FtsQuery::And(_)));
    }

    #[test]
    fn test_query_parse_or() {
        let q = FtsQuery::parse("hello OR world").unwrap();
        assert!(matches!(q, FtsQuery::Or(_)));
    }

    #[test]
    fn test_query_parse_phrase() {
        let q = FtsQuery::parse("\"hello world\"").unwrap();
        assert!(matches!(q, FtsQuery::Phrase(_)));
    }

    #[test]
    fn test_query_parse_fuzzy() {
        let q = FtsQuery::parse("hello~2").unwrap();
        assert!(matches!(q, FtsQuery::Fuzzy(_, 2)));
    }

    #[test]
    fn test_query_parse_empty() {
        assert!(FtsQuery::parse("").is_err());
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("abc", "ab"), 1);
    }

    #[test]
    fn test_highlight_snippet() {
        let text = "The quick brown fox jumps over the lazy dog";
        let snippet = highlight_snippet(text, &["quick".into(), "fox".into()], "<b>", "</b>", 200);
        assert!(snippet.contains("<b>quick</b>"));
        assert!(snippet.contains("<b>fox</b>"));
    }

    #[test]
    fn test_bm25_ranking_order() {
        let mut index = FtsIndex::new();
        let tokenizer = Tokenizer::raw();

        // Doc 0: "rust" appears 3 times
        index.add_document(0, "rust rust rust programming", &tokenizer);
        // Doc 1: "rust" appears 1 time
        index.add_document(1, "rust is a language for systems programming", &tokenizer);

        let results = index.search(&FtsQuery::Term("rust".into()), &tokenizer, 10);
        assert!(results.len() == 2);
        // Doc 0 should rank higher (more occurrences, shorter doc)
        assert_eq!(results[0].doc_id, 0);
    }

    #[test]
    fn test_index_manager() {
        let mut manager = FtsIndexManager::new();
        manager.create_index("products_description");
        manager.create_index("articles_body");

        assert_eq!(manager.index_count(), 2);
        assert!(manager.get_index("products_description").is_some());
        assert!(manager.get_index("nonexistent").is_none());

        assert!(manager.drop_index("products_description"));
        assert_eq!(manager.index_count(), 1);
        assert!(!manager.drop_index("products_description")); // already dropped
    }
}
