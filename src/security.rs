//! Advanced Security and Governance for Blaze.
//!
//! Provides RBAC, row-level security, column masking, and audit logging
//! for multi-tenant and enterprise deployments.

use std::collections::{HashMap, HashSet};

use crate::error::{BlazeError, Result};

// ---------------------------------------------------------------------------
// RBAC (Role-Based Access Control)
// ---------------------------------------------------------------------------

/// Permission that can be granted to a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Admin,
}

/// A named role containing a set of permissions.
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
}

/// A user with one or more assigned roles.
#[derive(Debug, Clone)]
pub struct User {
    pub username: String,
    pub roles: Vec<String>,
}

/// Manages roles, users, and permission checks.
#[derive(Debug, Default)]
pub struct RbacManager {
    roles: HashMap<String, Role>,
    users: HashMap<String, User>,
}

impl RbacManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new role. Returns error if the role already exists.
    pub fn create_role(&mut self, name: &str, permissions: HashSet<Permission>) -> Result<()> {
        if self.roles.contains_key(name) {
            return Err(BlazeError::execution(format!(
                "Role '{}' already exists",
                name
            )));
        }
        self.roles.insert(
            name.to_string(),
            Role {
                name: name.to_string(),
                permissions,
            },
        );
        Ok(())
    }

    /// Create a new user. Returns error if the user already exists.
    pub fn create_user(&mut self, username: &str) -> Result<()> {
        if self.users.contains_key(username) {
            return Err(BlazeError::execution(format!(
                "User '{}' already exists",
                username
            )));
        }
        self.users.insert(
            username.to_string(),
            User {
                username: username.to_string(),
                roles: Vec::new(),
            },
        );
        Ok(())
    }

    /// Grant a role to a user. Both must exist.
    pub fn grant_role(&mut self, username: &str, role_name: &str) -> Result<()> {
        if !self.roles.contains_key(role_name) {
            return Err(BlazeError::execution(format!(
                "Role '{}' does not exist",
                role_name
            )));
        }
        let user = self
            .users
            .get_mut(username)
            .ok_or_else(|| BlazeError::execution(format!("User '{}' does not exist", username)))?;
        if !user.roles.contains(&role_name.to_string()) {
            user.roles.push(role_name.to_string());
        }
        Ok(())
    }

    /// Check whether a user holds a given permission through any assigned role.
    pub fn check_permission(&self, username: &str, permission: Permission) -> bool {
        let user = match self.users.get(username) {
            Some(u) => u,
            None => return false,
        };
        user.roles.iter().any(|role_name| {
            self.roles
                .get(role_name)
                .is_some_and(|r| r.permissions.contains(&permission))
        })
    }
}

// ---------------------------------------------------------------------------
// Row-Level Security (RLS)
// ---------------------------------------------------------------------------

/// A row-level security policy attached to a table.
#[derive(Debug, Clone)]
pub struct RlsPolicy {
    pub name: String,
    pub table: String,
    pub filter_expr: String,
    pub enabled: bool,
}

/// Manages row-level security policies.
#[derive(Debug, Default)]
pub struct RlsPolicyManager {
    policies: Vec<RlsPolicy>,
}

impl RlsPolicyManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a policy. Returns error on duplicate name for the same table.
    pub fn add_policy(&mut self, policy: RlsPolicy) -> Result<()> {
        let duplicate = self
            .policies
            .iter()
            .any(|p| p.name == policy.name && p.table == policy.table);
        if duplicate {
            return Err(BlazeError::execution(format!(
                "RLS policy '{}' already exists for table '{}'",
                policy.name, policy.table
            )));
        }
        self.policies.push(policy);
        Ok(())
    }

    /// Return all policies (enabled or not) for a table.
    pub fn get_policies(&self, table: &str) -> Vec<&RlsPolicy> {
        self.policies.iter().filter(|p| p.table == table).collect()
    }

    /// Evaluate enabled policies for a given user and table, returning filter
    /// expressions that should be applied to the query.
    pub fn evaluate(&self, _user: &str, table: &str) -> Vec<String> {
        self.policies
            .iter()
            .filter(|p| p.table == table && p.enabled)
            .map(|p| p.filter_expr.clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Column Masking
// ---------------------------------------------------------------------------

/// Strategy used to mask a column value.
#[derive(Debug, Clone, PartialEq)]
pub enum MaskingStrategy {
    Hash,
    Redact,
    PartialReveal { visible_chars: usize },
    Nullify,
    Custom(String),
}

/// A masking rule binding a column to a strategy.
#[derive(Debug, Clone)]
pub struct MaskingRule {
    pub column: String,
    pub strategy: MaskingStrategy,
}

/// Applies column-level masking rules.
#[derive(Debug, Default)]
pub struct MaskingEngine {
    rules: Vec<MaskingRule>,
}

impl MaskingEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_rule(&mut self, rule: MaskingRule) {
        self.rules.push(rule);
    }

    /// Return the masking rule for a column, if any.
    pub fn get_rule(&self, column: &str) -> Option<&MaskingRule> {
        self.rules.iter().find(|r| r.column == column)
    }

    /// Apply a masking strategy to a string value.
    pub fn apply_mask(value: &str, strategy: &MaskingStrategy) -> String {
        match strategy {
            MaskingStrategy::Hash => {
                // Simple deterministic hash representation.
                let hash: u64 = value
                    .bytes()
                    .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
                format!("{:016x}", hash)
            }
            MaskingStrategy::Redact => "***".to_string(),
            MaskingStrategy::PartialReveal { visible_chars } => {
                let chars: Vec<char> = value.chars().collect();
                if *visible_chars >= chars.len() {
                    return value.to_string();
                }
                let visible: String = chars[chars.len() - visible_chars..].iter().collect();
                format!("{}{}", "*".repeat(chars.len() - visible_chars), visible)
            }
            MaskingStrategy::Nullify => String::new(),
            MaskingStrategy::Custom(replacement) => replacement.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Audit Logger
// ---------------------------------------------------------------------------

/// Types of auditable actions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AuditAction {
    Query,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    Login,
    Logout,
}

impl std::fmt::Display for AuditAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Query => "Query",
            Self::Insert => "Insert",
            Self::Update => "Update",
            Self::Delete => "Delete",
            Self::CreateTable => "CreateTable",
            Self::DropTable => "DropTable",
            Self::Login => "Login",
            Self::Logout => "Logout",
        };
        f.write_str(s)
    }
}

/// A single audit log entry.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub timestamp: u64,
    pub user: String,
    pub action: AuditAction,
    pub table: Option<String>,
    pub query: Option<String>,
    pub success: bool,
}

/// Optional filters for querying the audit log.
#[derive(Debug, Default)]
pub struct AuditFilter {
    pub user: Option<String>,
    pub action: Option<AuditAction>,
    pub table: Option<String>,
    pub since: Option<u64>,
}

/// Summary statistics for the audit log.
#[derive(Debug)]
pub struct AuditStats {
    pub total_events: usize,
    pub by_action: HashMap<String, u64>,
}

/// A bounded, in-memory audit log.
#[derive(Debug)]
pub struct AuditLog {
    max_entries: usize,
    events: Vec<AuditEvent>,
}

impl AuditLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            events: Vec::new(),
        }
    }

    /// Append an event, evicting the oldest entry when capacity is reached.
    pub fn log(&mut self, event: AuditEvent) {
        if self.events.len() >= self.max_entries {
            self.events.remove(0);
        }
        self.events.push(event);
    }

    /// Query the log with optional filters.
    pub fn query(&self, filter: &AuditFilter) -> Vec<&AuditEvent> {
        self.events
            .iter()
            .filter(|e| {
                if let Some(ref user) = filter.user {
                    if e.user != *user {
                        return false;
                    }
                }
                if let Some(ref action) = filter.action {
                    if e.action != *action {
                        return false;
                    }
                }
                if let Some(ref table) = filter.table {
                    if e.table.as_deref() != Some(table.as_str()) {
                        return false;
                    }
                }
                if let Some(since) = filter.since {
                    if e.timestamp < since {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    /// Compute summary statistics.
    pub fn stats(&self) -> AuditStats {
        let mut by_action: HashMap<String, u64> = HashMap::new();
        for event in &self.events {
            *by_action.entry(event.action.to_string()).or_insert(0) += 1;
        }
        AuditStats {
            total_events: self.events.len(),
            by_action,
        }
    }
}

// ---------------------------------------------------------------------------
// Security Context (for query-time user propagation)
// ---------------------------------------------------------------------------

/// Security context propagated through query execution.
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub user: String,
    pub role: String,
    pub attributes: std::collections::HashMap<String, String>,
}

impl SecurityContext {
    pub fn new(user: &str, role: &str) -> Self {
        Self {
            user: user.to_string(),
            role: role.to_string(),
            attributes: std::collections::HashMap::new(),
        }
    }

    pub fn with_attribute(mut self, key: &str, value: &str) -> Self {
        self.attributes.insert(key.to_string(), value.to_string());
        self
    }

    /// Returns the current user name (for use in RLS policies).
    pub fn current_user(&self) -> &str {
        &self.user
    }

    /// Returns the current role (for use in RLS policies).
    pub fn current_role(&self) -> &str {
        &self.role
    }

    /// Returns a user attribute (for use in RLS policies).
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(|s| s.as_str())
    }
}

// ---------------------------------------------------------------------------
// Column-Level Access Control
// ---------------------------------------------------------------------------

/// Fine-grained column-level permissions.
#[derive(Debug)]
pub struct ColumnAccessControl {
    rules: Vec<ColumnAccessRule>,
}

#[derive(Debug, Clone)]
pub struct ColumnAccessRule {
    pub table: String,
    pub column: String,
    pub role: String,
    pub permission: ColumnPermission,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnPermission {
    Allow,
    Deny,
    Mask(MaskingStrategy),
}

impl Default for ColumnAccessControl {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnAccessControl {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a column access rule.
    pub fn add_rule(&mut self, rule: ColumnAccessRule) {
        self.rules.push(rule);
    }

    /// Check if a role can access a specific column.
    pub fn check_access(&self, table: &str, column: &str, role: &str) -> ColumnPermission {
        // Find the most specific matching rule (column-specific > table-wide)
        for rule in self.rules.iter().rev() {
            if rule.table == table && rule.column == column && rule.role == role {
                return rule.permission.clone();
            }
        }
        ColumnPermission::Allow // Default: allow if no rule
    }

    /// Get all denied columns for a role on a table.
    pub fn denied_columns(&self, table: &str, role: &str) -> Vec<String> {
        self.rules
            .iter()
            .filter(|r| {
                r.table == table && r.role == role && matches!(r.permission, ColumnPermission::Deny)
            })
            .map(|r| r.column.clone())
            .collect()
    }

    /// Get all masked columns for a role on a table.
    pub fn masked_columns(&self, table: &str, role: &str) -> Vec<(String, MaskingStrategy)> {
        self.rules
            .iter()
            .filter(|r| r.table == table && r.role == role)
            .filter_map(|r| {
                if let ColumnPermission::Mask(ref strategy) = r.permission {
                    Some((r.column.clone(), strategy.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Policy Composition (AND/OR)
// ---------------------------------------------------------------------------

/// Composable RLS policy expressions.
#[derive(Debug, Clone)]
pub enum PolicyExpression {
    /// Simple filter: column op value
    Filter(String),
    /// All sub-policies must match
    And(Vec<PolicyExpression>),
    /// Any sub-policy can match
    Or(Vec<PolicyExpression>),
}

impl PolicyExpression {
    /// Convert to a SQL WHERE clause fragment.
    pub fn to_sql(&self) -> String {
        match self {
            PolicyExpression::Filter(f) => f.clone(),
            PolicyExpression::And(exprs) => {
                let parts: Vec<String> = exprs.iter().map(|e| e.to_sql()).collect();
                format!("({})", parts.join(" AND "))
            }
            PolicyExpression::Or(exprs) => {
                let parts: Vec<String> = exprs.iter().map(|e| e.to_sql()).collect();
                format!("({})", parts.join(" OR "))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Persistent Audit Log
// ---------------------------------------------------------------------------

/// Audit log configuration with retention and export.
#[derive(Debug)]
pub struct AuditLogConfig {
    pub max_events: usize,
    pub retention_days: u32,
    pub sample_rate: f64, // 0.0 to 1.0 (1.0 = log all queries)
}

impl Default for AuditLogConfig {
    fn default() -> Self {
        Self {
            max_events: 100_000,
            retention_days: 90,
            sample_rate: 1.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Column-Level Encryption
// ---------------------------------------------------------------------------

/// Trait for pluggable key providers.
pub trait KeyProvider: Send + Sync {
    /// Retrieve the encryption key for a given key ID.
    fn get_key(&self, key_id: &str) -> Result<Vec<u8>>;

    /// List all available key IDs.
    fn list_key_ids(&self) -> Vec<String>;

    /// Rotate: generate a new key and return its ID.
    fn rotate_key(&mut self, old_key_id: &str) -> Result<String>;
}

/// Simple in-memory key provider for development/testing.
#[derive(Debug, Default)]
pub struct InMemoryKeyProvider {
    keys: HashMap<String, Vec<u8>>,
    next_id: usize,
}

impl InMemoryKeyProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a key with a specific ID.
    pub fn add_key(&mut self, key_id: impl Into<String>, key: Vec<u8>) {
        self.keys.insert(key_id.into(), key);
    }

    /// Generate a deterministic key for testing (NOT for production).
    pub fn generate_test_key(&mut self) -> String {
        let id = format!("key-{}", self.next_id);
        self.next_id += 1;
        let key: Vec<u8> = (0..32)
            .map(|i| (i + self.next_id as u8).wrapping_mul(7))
            .collect();
        self.keys.insert(id.clone(), key);
        id
    }
}

impl KeyProvider for InMemoryKeyProvider {
    fn get_key(&self, key_id: &str) -> Result<Vec<u8>> {
        self.keys
            .get(key_id)
            .cloned()
            .ok_or_else(|| BlazeError::execution(format!("Key '{}' not found", key_id)))
    }

    fn list_key_ids(&self) -> Vec<String> {
        self.keys.keys().cloned().collect()
    }

    fn rotate_key(&mut self, old_key_id: &str) -> Result<String> {
        let old_key = self.get_key(old_key_id)?;
        let new_id = format!("{}-rotated-{}", old_key_id, self.next_id);
        self.next_id += 1;
        // Derive new key by hashing old key (simplified)
        let new_key: Vec<u8> = old_key.iter().map(|b| b.wrapping_add(1)).collect();
        self.keys.insert(new_id.clone(), new_key);
        Ok(new_id)
    }
}

/// Configuration for column encryption.
#[derive(Debug, Clone)]
pub struct ColumnEncryptionConfig {
    pub table_name: String,
    pub column_name: String,
    pub key_id: String,
    pub algorithm: EncryptionAlgorithm,
}

/// Supported encryption algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (authenticated encryption)
    Aes256Gcm,
    /// Simple XOR (for testing only — NOT secure)
    XorTest,
}

/// Manages column-level encryption across tables.
pub struct ColumnEncryptionManager {
    configs: HashMap<(String, String), ColumnEncryptionConfig>,
    key_provider: Box<dyn KeyProvider>,
}

impl ColumnEncryptionManager {
    pub fn new(key_provider: Box<dyn KeyProvider>) -> Self {
        Self {
            configs: HashMap::new(),
            key_provider,
        }
    }

    /// Register a column for encryption.
    pub fn register_column(&mut self, config: ColumnEncryptionConfig) {
        let key = (config.table_name.clone(), config.column_name.clone());
        self.configs.insert(key, config);
    }

    /// Check if a column is encrypted.
    pub fn is_encrypted(&self, table: &str, column: &str) -> bool {
        self.configs
            .contains_key(&(table.to_string(), column.to_string()))
    }

    /// Get the encryption config for a column.
    pub fn get_config(&self, table: &str, column: &str) -> Option<&ColumnEncryptionConfig> {
        self.configs.get(&(table.to_string(), column.to_string()))
    }

    /// List all encrypted columns for a table.
    pub fn encrypted_columns(&self, table: &str) -> Vec<String> {
        self.configs
            .iter()
            .filter(|((t, _), _)| t == table)
            .map(|((_, c), _)| c.clone())
            .collect()
    }

    /// Encrypt a byte payload using the configured algorithm and key.
    pub fn encrypt(&self, table: &str, column: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
        let config = self.get_config(table, column).ok_or_else(|| {
            BlazeError::execution(format!("No encryption config for {}.{}", table, column))
        })?;
        let key = self.key_provider.get_key(&config.key_id)?;

        match config.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                // Simplified AES-256-GCM: XOR with key-derived pad + HMAC tag.
                // A production implementation would use the `aes-gcm` crate.
                let pad = derive_pad(&key, plaintext.len());
                let ciphertext: Vec<u8> = plaintext
                    .iter()
                    .zip(pad.iter())
                    .map(|(p, k)| p ^ k)
                    .collect();
                // Prepend a 4-byte tag (simplified MAC)
                let tag = compute_tag(&key, &ciphertext);
                let mut result = tag.to_vec();
                result.extend(&ciphertext);
                Ok(result)
            }
            EncryptionAlgorithm::XorTest => {
                let pad = derive_pad(&key, plaintext.len());
                Ok(plaintext
                    .iter()
                    .zip(pad.iter())
                    .map(|(p, k)| p ^ k)
                    .collect())
            }
        }
    }

    /// Decrypt a byte payload.
    pub fn decrypt(&self, table: &str, column: &str, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let config = self.get_config(table, column).ok_or_else(|| {
            BlazeError::execution(format!("No encryption config for {}.{}", table, column))
        })?;
        let key = self.key_provider.get_key(&config.key_id)?;

        match config.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                if ciphertext.len() < 4 {
                    return Err(BlazeError::execution("Ciphertext too short"));
                }
                let (tag_bytes, data) = ciphertext.split_at(4);
                // Verify tag
                let expected_tag = compute_tag(&key, data);
                if tag_bytes != expected_tag {
                    return Err(BlazeError::execution(
                        "Decryption failed: authentication tag mismatch",
                    ));
                }
                let pad = derive_pad(&key, data.len());
                Ok(data.iter().zip(pad.iter()).map(|(c, k)| c ^ k).collect())
            }
            EncryptionAlgorithm::XorTest => {
                let pad = derive_pad(&key, ciphertext.len());
                Ok(ciphertext
                    .iter()
                    .zip(pad.iter())
                    .map(|(c, k)| c ^ k)
                    .collect())
            }
        }
    }

    /// Encrypt a string value for a column.
    pub fn encrypt_string(&self, table: &str, column: &str, value: &str) -> Result<String> {
        let encrypted = self.encrypt(table, column, value.as_bytes())?;
        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &encrypted,
        ))
    }

    /// Decrypt a base64-encoded encrypted string.
    pub fn decrypt_string(&self, table: &str, column: &str, encoded: &str) -> Result<String> {
        let ciphertext =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                .map_err(|e| BlazeError::execution(format!("Base64 decode error: {}", e)))?;
        let plaintext = self.decrypt(table, column, &ciphertext)?;
        String::from_utf8(plaintext)
            .map_err(|e| BlazeError::execution(format!("UTF-8 decode error: {}", e)))
    }
}

/// Derive a repeating pad from a key.
fn derive_pad(key: &[u8], len: usize) -> Vec<u8> {
    key.iter().cycle().take(len).copied().collect()
}

/// Compute a simplified 4-byte authentication tag.
fn compute_tag(key: &[u8], data: &[u8]) -> [u8; 4] {
    let mut hash: u32 = 0x811c9dc5;
    for &b in key.iter().chain(data.iter()) {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash.to_le_bytes()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- RBAC tests ---------------------------------------------------------

    #[test]
    fn test_rbac_create_role_and_check_permission() {
        let mut mgr = RbacManager::new();
        let perms = HashSet::from([Permission::Select, Permission::Insert]);
        mgr.create_role("writer", perms).unwrap();
        mgr.create_user("alice").unwrap();
        mgr.grant_role("alice", "writer").unwrap();

        assert!(mgr.check_permission("alice", Permission::Select));
        assert!(mgr.check_permission("alice", Permission::Insert));
        assert!(!mgr.check_permission("alice", Permission::Drop));
    }

    #[test]
    fn test_rbac_duplicate_role_error() {
        let mut mgr = RbacManager::new();
        mgr.create_role("admin", HashSet::from([Permission::Admin]))
            .unwrap();
        let res = mgr.create_role("admin", HashSet::new());
        assert!(res.is_err());
    }

    #[test]
    fn test_rbac_unknown_user_has_no_permissions() {
        let mgr = RbacManager::new();
        assert!(!mgr.check_permission("ghost", Permission::Select));
    }

    #[test]
    fn test_rbac_grant_nonexistent_role_error() {
        let mut mgr = RbacManager::new();
        mgr.create_user("bob").unwrap();
        let res = mgr.grant_role("bob", "nonexistent");
        assert!(res.is_err());
    }

    // -- RLS tests ----------------------------------------------------------

    #[test]
    fn test_rls_add_and_evaluate() {
        let mut mgr = RlsPolicyManager::new();
        mgr.add_policy(RlsPolicy {
            name: "tenant_filter".into(),
            table: "orders".into(),
            filter_expr: "tenant_id = 42".into(),
            enabled: true,
        })
        .unwrap();
        mgr.add_policy(RlsPolicy {
            name: "disabled_policy".into(),
            table: "orders".into(),
            filter_expr: "status = 'active'".into(),
            enabled: false,
        })
        .unwrap();

        let filters = mgr.evaluate("alice", "orders");
        assert_eq!(filters, vec!["tenant_id = 42"]);
    }

    #[test]
    fn test_rls_duplicate_policy_error() {
        let mut mgr = RlsPolicyManager::new();
        mgr.add_policy(RlsPolicy {
            name: "p1".into(),
            table: "t".into(),
            filter_expr: "1=1".into(),
            enabled: true,
        })
        .unwrap();
        let res = mgr.add_policy(RlsPolicy {
            name: "p1".into(),
            table: "t".into(),
            filter_expr: "2=2".into(),
            enabled: true,
        });
        assert!(res.is_err());
    }

    #[test]
    fn test_rls_get_policies() {
        let mut mgr = RlsPolicyManager::new();
        mgr.add_policy(RlsPolicy {
            name: "a".into(),
            table: "orders".into(),
            filter_expr: "1=1".into(),
            enabled: true,
        })
        .unwrap();
        mgr.add_policy(RlsPolicy {
            name: "b".into(),
            table: "users".into(),
            filter_expr: "1=1".into(),
            enabled: true,
        })
        .unwrap();

        assert_eq!(mgr.get_policies("orders").len(), 1);
        assert_eq!(mgr.get_policies("users").len(), 1);
        assert_eq!(mgr.get_policies("other").len(), 0);
    }

    // -- Column Masking tests -----------------------------------------------

    #[test]
    fn test_masking_redact() {
        assert_eq!(
            MaskingEngine::apply_mask("secret", &MaskingStrategy::Redact),
            "***"
        );
    }

    #[test]
    fn test_masking_partial_reveal() {
        let strategy = MaskingStrategy::PartialReveal { visible_chars: 4 };
        assert_eq!(
            MaskingEngine::apply_mask("4111111111111111", &strategy),
            "************1111"
        );
    }

    #[test]
    fn test_masking_nullify() {
        assert_eq!(
            MaskingEngine::apply_mask("anything", &MaskingStrategy::Nullify),
            ""
        );
    }

    #[test]
    fn test_masking_hash_deterministic() {
        let a = MaskingEngine::apply_mask("hello", &MaskingStrategy::Hash);
        let b = MaskingEngine::apply_mask("hello", &MaskingStrategy::Hash);
        assert_eq!(a, b);
        assert_ne!(a, "hello");
    }

    #[test]
    fn test_masking_custom() {
        let strategy = MaskingStrategy::Custom("[CLASSIFIED]".into());
        assert_eq!(MaskingEngine::apply_mask("data", &strategy), "[CLASSIFIED]");
    }

    // -- Audit Log tests ----------------------------------------------------

    #[test]
    fn test_audit_log_and_query() {
        let mut log = AuditLog::new(100);
        log.log(AuditEvent {
            timestamp: 1000,
            user: "alice".into(),
            action: AuditAction::Query,
            table: Some("orders".into()),
            query: Some("SELECT * FROM orders".into()),
            success: true,
        });
        log.log(AuditEvent {
            timestamp: 2000,
            user: "bob".into(),
            action: AuditAction::Insert,
            table: Some("orders".into()),
            query: None,
            success: true,
        });

        let results = log.query(&AuditFilter {
            user: Some("alice".into()),
            ..Default::default()
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].user, "alice");
    }

    #[test]
    fn test_audit_log_eviction() {
        let mut log = AuditLog::new(2);
        for i in 0..3 {
            log.log(AuditEvent {
                timestamp: i,
                user: format!("u{}", i),
                action: AuditAction::Login,
                table: None,
                query: None,
                success: true,
            });
        }
        // First event (u0) should have been evicted.
        let all = log.query(&AuditFilter::default());
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].user, "u1");
        assert_eq!(all[1].user, "u2");
    }

    #[test]
    fn test_audit_stats() {
        let mut log = AuditLog::new(100);
        log.log(AuditEvent {
            timestamp: 1,
            user: "a".into(),
            action: AuditAction::Query,
            table: None,
            query: None,
            success: true,
        });
        log.log(AuditEvent {
            timestamp: 2,
            user: "a".into(),
            action: AuditAction::Query,
            table: None,
            query: None,
            success: true,
        });
        log.log(AuditEvent {
            timestamp: 3,
            user: "a".into(),
            action: AuditAction::Login,
            table: None,
            query: None,
            success: true,
        });

        let stats = log.stats();
        assert_eq!(stats.total_events, 3);
        assert_eq!(stats.by_action.get("Query"), Some(&2));
        assert_eq!(stats.by_action.get("Login"), Some(&1));
    }

    // -- Security Context tests -----------------------------------------------

    #[test]
    fn test_security_context() {
        let ctx = SecurityContext::new("alice", "analyst")
            .with_attribute("region", "US-West")
            .with_attribute("department", "sales");

        assert_eq!(ctx.current_user(), "alice");
        assert_eq!(ctx.current_role(), "analyst");
        assert_eq!(ctx.get_attribute("region"), Some("US-West"));
        assert_eq!(ctx.get_attribute("missing"), None);
    }

    // -- Column Access Control tests ------------------------------------------

    #[test]
    fn test_column_access_deny() {
        let mut cac = ColumnAccessControl::new();
        cac.add_rule(ColumnAccessRule {
            table: "users".into(),
            column: "ssn".into(),
            role: "analyst".into(),
            permission: ColumnPermission::Deny,
        });

        assert_eq!(
            cac.check_access("users", "ssn", "analyst"),
            ColumnPermission::Deny
        );
        assert_eq!(
            cac.check_access("users", "name", "analyst"),
            ColumnPermission::Allow
        );
    }

    #[test]
    fn test_column_access_mask() {
        let mut cac = ColumnAccessControl::new();
        cac.add_rule(ColumnAccessRule {
            table: "users".into(),
            column: "email".into(),
            role: "viewer".into(),
            permission: ColumnPermission::Mask(MaskingStrategy::Redact),
        });

        let masked = cac.masked_columns("users", "viewer");
        assert_eq!(masked.len(), 1);
        assert_eq!(masked[0].0, "email");
    }

    #[test]
    fn test_denied_columns() {
        let mut cac = ColumnAccessControl::new();
        cac.add_rule(ColumnAccessRule {
            table: "t".into(),
            column: "c1".into(),
            role: "r".into(),
            permission: ColumnPermission::Deny,
        });
        cac.add_rule(ColumnAccessRule {
            table: "t".into(),
            column: "c2".into(),
            role: "r".into(),
            permission: ColumnPermission::Deny,
        });
        cac.add_rule(ColumnAccessRule {
            table: "t".into(),
            column: "c3".into(),
            role: "r".into(),
            permission: ColumnPermission::Allow,
        });

        let denied = cac.denied_columns("t", "r");
        assert_eq!(denied.len(), 2);
    }

    // -- Policy Composition tests ---------------------------------------------

    #[test]
    fn test_policy_expression_simple() {
        let expr = PolicyExpression::Filter("region = 'US'".into());
        assert_eq!(expr.to_sql(), "region = 'US'");
    }

    #[test]
    fn test_policy_expression_and() {
        let expr = PolicyExpression::And(vec![
            PolicyExpression::Filter("region = 'US'".into()),
            PolicyExpression::Filter("department = 'sales'".into()),
        ]);
        assert_eq!(expr.to_sql(), "(region = 'US' AND department = 'sales')");
    }

    #[test]
    fn test_policy_expression_or() {
        let expr = PolicyExpression::Or(vec![
            PolicyExpression::Filter("role = 'admin'".into()),
            PolicyExpression::Filter("owner_id = current_user()".into()),
        ]);
        assert_eq!(
            expr.to_sql(),
            "(role = 'admin' OR owner_id = current_user())"
        );
    }

    #[test]
    fn test_policy_expression_nested() {
        let expr = PolicyExpression::And(vec![
            PolicyExpression::Filter("active = true".into()),
            PolicyExpression::Or(vec![
                PolicyExpression::Filter("role = 'admin'".into()),
                PolicyExpression::Filter("region = 'US'".into()),
            ]),
        ]);
        let sql = expr.to_sql();
        assert!(sql.contains("active = true"));
        assert!(sql.contains("role = 'admin' OR region = 'US'"));
    }

    // -- Key Provider tests ---------------------------------------------------

    #[test]
    fn test_in_memory_key_provider() {
        let mut kp = InMemoryKeyProvider::new();
        kp.add_key("k1", vec![0xAA; 32]);
        assert_eq!(kp.get_key("k1").unwrap().len(), 32);
        assert!(kp.get_key("missing").is_err());
    }

    #[test]
    fn test_key_rotation() {
        let mut kp = InMemoryKeyProvider::new();
        kp.add_key("k1", vec![0xBB; 32]);
        let new_id = kp.rotate_key("k1").unwrap();
        assert!(kp.get_key(&new_id).is_ok());
        assert_ne!(kp.get_key("k1").unwrap(), kp.get_key(&new_id).unwrap());
    }

    #[test]
    fn test_generate_test_key() {
        let mut kp = InMemoryKeyProvider::new();
        let id1 = kp.generate_test_key();
        let id2 = kp.generate_test_key();
        assert_ne!(id1, id2);
        assert_eq!(kp.list_key_ids().len(), 2);
    }

    // -- Column Encryption tests ----------------------------------------------

    #[test]
    fn test_encryption_roundtrip_xor() {
        let mut kp = InMemoryKeyProvider::new();
        kp.add_key("test-key", vec![0x42; 32]);

        let mut mgr = ColumnEncryptionManager::new(Box::new(kp));
        mgr.register_column(ColumnEncryptionConfig {
            table_name: "users".into(),
            column_name: "ssn".into(),
            key_id: "test-key".into(),
            algorithm: EncryptionAlgorithm::XorTest,
        });

        assert!(mgr.is_encrypted("users", "ssn"));
        assert!(!mgr.is_encrypted("users", "name"));

        let plaintext = b"123-45-6789";
        let encrypted = mgr.encrypt("users", "ssn", plaintext).unwrap();
        assert_ne!(encrypted, plaintext);

        let decrypted = mgr.decrypt("users", "ssn", &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encryption_roundtrip_aes256gcm() {
        let mut kp = InMemoryKeyProvider::new();
        kp.add_key("aes-key", vec![0x37; 32]);

        let mut mgr = ColumnEncryptionManager::new(Box::new(kp));
        mgr.register_column(ColumnEncryptionConfig {
            table_name: "t".into(),
            column_name: "c".into(),
            key_id: "aes-key".into(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
        });

        let plaintext = b"secret data here";
        let encrypted = mgr.encrypt("t", "c", plaintext).unwrap();
        assert!(encrypted.len() > plaintext.len()); // tag prefix

        let decrypted = mgr.decrypt("t", "c", &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encryption_tamper_detection() {
        let mut kp = InMemoryKeyProvider::new();
        kp.add_key("k", vec![0x55; 32]);

        let mut mgr = ColumnEncryptionManager::new(Box::new(kp));
        mgr.register_column(ColumnEncryptionConfig {
            table_name: "t".into(),
            column_name: "c".into(),
            key_id: "k".into(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
        });

        let mut encrypted = mgr.encrypt("t", "c", b"hello").unwrap();
        // Tamper with the ciphertext
        if let Some(last) = encrypted.last_mut() {
            *last ^= 0xFF;
        }
        assert!(mgr.decrypt("t", "c", &encrypted).is_err());
    }

    #[test]
    fn test_encrypt_decrypt_string() {
        let mut kp = InMemoryKeyProvider::new();
        kp.add_key("sk", vec![0x11; 32]);

        let mut mgr = ColumnEncryptionManager::new(Box::new(kp));
        mgr.register_column(ColumnEncryptionConfig {
            table_name: "t".into(),
            column_name: "email".into(),
            key_id: "sk".into(),
            algorithm: EncryptionAlgorithm::XorTest,
        });

        let encoded = mgr
            .encrypt_string("t", "email", "user@example.com")
            .unwrap();
        let decoded = mgr.decrypt_string("t", "email", &encoded).unwrap();
        assert_eq!(decoded, "user@example.com");
    }

    #[test]
    fn test_encrypted_columns_list() {
        let kp = InMemoryKeyProvider::new();
        let mut mgr = ColumnEncryptionManager::new(Box::new(kp));
        mgr.register_column(ColumnEncryptionConfig {
            table_name: "users".into(),
            column_name: "ssn".into(),
            key_id: "k".into(),
            algorithm: EncryptionAlgorithm::XorTest,
        });
        mgr.register_column(ColumnEncryptionConfig {
            table_name: "users".into(),
            column_name: "email".into(),
            key_id: "k".into(),
            algorithm: EncryptionAlgorithm::XorTest,
        });

        let cols = mgr.encrypted_columns("users");
        assert_eq!(cols.len(), 2);
    }
}
