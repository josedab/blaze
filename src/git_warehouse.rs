//! Git-native data warehouse for version-controlled data.
//!
//! This module provides a git-native data source that reads CSV and Parquet files
//! from git repositories at specific refs (branches, tags, commits).

use crate::error::{BlazeError, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Git reference types.
#[derive(Debug, Clone, PartialEq)]
pub enum GitRef {
    /// A branch name (e.g., "main", "develop")
    Branch(String),
    /// A tag name (e.g., "v1.0.0")
    Tag(String),
    /// A specific commit SHA
    Commit(String),
    /// Current HEAD
    Head,
}

impl GitRef {
    /// Convert the GitRef to a string usable by git commands.
    pub fn to_git_ref_string(&self) -> String {
        match self {
            GitRef::Branch(name) => name.clone(),
            GitRef::Tag(name) => name.clone(),
            GitRef::Commit(sha) => sha.clone(),
            GitRef::Head => "HEAD".to_string(),
        }
    }

    /// Parse a string into a GitRef.
    pub fn parse(s: &str) -> Self {
        if s.eq_ignore_ascii_case("head") {
            GitRef::Head
        } else if s.starts_with("refs/heads/") {
            GitRef::Branch(s.strip_prefix("refs/heads/").unwrap().to_string())
        } else if s.starts_with("refs/tags/") {
            GitRef::Tag(s.strip_prefix("refs/tags/").unwrap().to_string())
        } else if s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            // Looks like a commit SHA
            GitRef::Commit(s.to_string())
        } else {
            // Default to branch
            GitRef::Branch(s.to_string())
        }
    }
}

impl std::fmt::Display for GitRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_git_ref_string())
    }
}

/// Git-native data source.
pub struct GitDataSource {
    /// Path to the git repository
    pub repo_path: PathBuf,
    /// Default reference to use
    pub default_ref: GitRef,
}

impl GitDataSource {
    /// Create a new GitDataSource.
    pub fn new(repo_path: impl AsRef<Path>) -> Result<Self> {
        let repo_path = repo_path.as_ref().to_path_buf();

        // Verify it's a git repository
        if !repo_path.join(".git").exists() {
            return Err(BlazeError::Io {
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Not a git repository: {}", repo_path.display()),
                ),
            });
        }

        Ok(Self {
            repo_path,
            default_ref: GitRef::Head,
        })
    }

    /// Set the default ref.
    pub fn with_default_ref(mut self, git_ref: GitRef) -> Self {
        self.default_ref = git_ref;
        self
    }

    /// List data files (CSV/Parquet) at a specific ref.
    pub fn list_data_files(&self, git_ref: &GitRef) -> Result<Vec<String>> {
        let ref_str = git_ref.to_git_ref_string();

        let output = Command::new("git")
            .arg("-C")
            .arg(&self.repo_path)
            .arg("ls-tree")
            .arg("-r")
            .arg("--name-only")
            .arg(&ref_str)
            .output()
            .map_err(|e| BlazeError::Execution {
                message: format!("Failed to execute git ls-tree: {}", e),
            })?;

        if !output.status.success() {
            return Err(BlazeError::Execution {
                message: format!(
                    "git ls-tree failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            });
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let files: Vec<String> = stdout
            .lines()
            .filter(|line| {
                line.ends_with(".csv")
                    || line.ends_with(".parquet")
                    || line.ends_with(".CSV")
                    || line.ends_with(".PARQUET")
            })
            .map(|s| s.to_string())
            .collect();

        Ok(files)
    }

    /// Read file content at a specific ref.
    pub fn read_file_at_ref(&self, path: &str, git_ref: &GitRef) -> Result<Vec<u8>> {
        let ref_str = git_ref.to_git_ref_string();
        let object_spec = format!("{}:{}", ref_str, path);

        let output = Command::new("git")
            .arg("-C")
            .arg(&self.repo_path)
            .arg("show")
            .arg(&object_spec)
            .output()
            .map_err(|e| BlazeError::Execution {
                message: format!("Failed to execute git show: {}", e),
            })?;

        if !output.status.success() {
            return Err(BlazeError::Execution {
                message: format!(
                    "git show failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            });
        }

        Ok(output.stdout)
    }

    /// Check if a ref exists.
    pub fn ref_exists(&self, git_ref: &GitRef) -> bool {
        let ref_str = git_ref.to_git_ref_string();

        Command::new("git")
            .arg("-C")
            .arg(&self.repo_path)
            .arg("rev-parse")
            .arg("--verify")
            .arg(&ref_str)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}

/// Table provider that reads from git refs.
pub struct GitTableProvider {
    source: GitDataSource,
    file_path: String,
    git_ref: GitRef,
}

impl GitTableProvider {
    pub fn new(source: GitDataSource, file_path: impl Into<String>, git_ref: GitRef) -> Self {
        Self {
            source,
            file_path: file_path.into(),
            git_ref,
        }
    }

    /// Read the file content.
    pub fn read_content(&self) -> Result<Vec<u8>> {
        self.source.read_file_at_ref(&self.file_path, &self.git_ref)
    }

    /// Get the file path.
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Get the git ref.
    pub fn git_ref(&self) -> &GitRef {
        &self.git_ref
    }
}

/// Report comparing data between two git refs.
#[derive(Debug, Clone)]
pub struct DiffReport {
    /// File path being compared
    pub file_path: String,
    /// Old ref
    pub old_ref: GitRef,
    /// New ref
    pub new_ref: GitRef,
    /// Estimated rows added
    pub rows_added: usize,
    /// Estimated rows removed
    pub rows_removed: usize,
    /// Schema changes detected
    pub schema_changes: Vec<String>,
}

impl DiffReport {
    pub fn new(file_path: String, old_ref: GitRef, new_ref: GitRef) -> Self {
        Self {
            file_path,
            old_ref,
            new_ref,
            rows_added: 0,
            rows_removed: 0,
            schema_changes: Vec::new(),
        }
    }

    /// Get a summary string.
    pub fn summary(&self) -> String {
        format!(
            "Diff {}: {} → {} (+{} rows, -{} rows, {} schema changes)",
            self.file_path,
            self.old_ref,
            self.new_ref,
            self.rows_added,
            self.rows_removed,
            self.schema_changes.len()
        )
    }
}

/// Compare data files between two git refs.
pub fn compare_refs(
    source: &GitDataSource,
    file_path: &str,
    old_ref: &GitRef,
    new_ref: &GitRef,
) -> Result<DiffReport> {
    let mut report = DiffReport::new(file_path.to_string(), old_ref.clone(), new_ref.clone());

    // Read file content at both refs
    let old_content = source.read_file_at_ref(file_path, old_ref)?;
    let new_content = source.read_file_at_ref(file_path, new_ref)?;

    // Simple heuristic: count lines for CSV files
    if file_path.ends_with(".csv") || file_path.ends_with(".CSV") {
        let old_lines = old_content.split(|&b| b == b'\n').count();
        let new_lines = new_content.split(|&b| b == b'\n').count();

        if new_lines > old_lines {
            report.rows_added = new_lines - old_lines;
        } else {
            report.rows_removed = old_lines - new_lines;
        }

        // Check for schema changes (header line)
        let old_header = old_content.split(|&b| b == b'\n').next().unwrap_or(&[]);
        let new_header = new_content.split(|&b| b == b'\n').next().unwrap_or(&[]);

        if old_header != new_header {
            report.schema_changes.push("CSV header changed".to_string());
        }
    } else if file_path.ends_with(".parquet") || file_path.ends_with(".PARQUET") {
        // For Parquet, we'd need to parse the file format to get accurate counts
        // For now, just report file size change as a proxy
        if old_content.len() != new_content.len() {
            report.schema_changes.push(format!(
                "File size changed: {} → {} bytes",
                old_content.len(),
                new_content.len()
            ));
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_git_ref_parse_branch() {
        let git_ref = GitRef::parse("main");
        assert_eq!(git_ref, GitRef::Branch("main".to_string()));
    }

    #[test]
    fn test_git_ref_parse_tag() {
        let git_ref = GitRef::parse("refs/tags/v1.0.0");
        assert_eq!(git_ref, GitRef::Tag("v1.0.0".to_string()));
    }

    #[test]
    fn test_git_ref_parse_commit() {
        let git_ref = GitRef::parse("a1b2c3d4e5f6789012345678901234567890abcd");
        assert!(matches!(git_ref, GitRef::Commit(_)));
    }

    #[test]
    fn test_git_ref_parse_head() {
        let git_ref = GitRef::parse("HEAD");
        assert_eq!(git_ref, GitRef::Head);
    }

    #[test]
    fn test_git_ref_to_string() {
        assert_eq!(GitRef::Branch("main".into()).to_git_ref_string(), "main");
        assert_eq!(GitRef::Tag("v1.0".into()).to_git_ref_string(), "v1.0");
        assert_eq!(GitRef::Head.to_git_ref_string(), "HEAD");
    }

    #[test]
    fn test_diff_report() {
        let mut report = DiffReport::new(
            "data.csv".to_string(),
            GitRef::Branch("main".into()),
            GitRef::Branch("dev".into()),
        );
        report.rows_added = 100;
        report.rows_removed = 10;
        report.schema_changes.push("Column added".to_string());

        let summary = report.summary();
        assert!(summary.contains("data.csv"));
        assert!(summary.contains("+100"));
        assert!(summary.contains("-10"));
        assert!(summary.contains("1 schema changes"));
    }

    #[test]
    fn test_git_table_provider() {
        // This test would require an actual git repository, so we just test construction
        // In a real test, you'd create a temp git repo
        let result = GitDataSource::new("/nonexistent/path");
        assert!(result.is_err());
    }

    #[test]
    fn test_git_ref_display() {
        let branch = GitRef::Branch("feature".into());
        assert_eq!(format!("{}", branch), "feature");

        let tag = GitRef::Tag("v2.0".into());
        assert_eq!(format!("{}", tag), "v2.0");
    }
}
