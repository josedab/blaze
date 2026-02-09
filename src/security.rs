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
            return Err(BlazeError::execution(format!("Role '{}' already exists", name)));
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
        let user = self.users.get_mut(username).ok_or_else(|| {
            BlazeError::execution(format!("User '{}' does not exist", username))
        })?;
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
                .map_or(false, |r| r.permissions.contains(&permission))
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
                let hash: u64 = value.bytes().fold(0u64, |acc, b| {
                    acc.wrapping_mul(31).wrapping_add(b as u64)
                });
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
        assert_eq!(MaskingEngine::apply_mask("secret", &MaskingStrategy::Redact), "***");
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
        assert_eq!(MaskingEngine::apply_mask("anything", &MaskingStrategy::Nullify), "");
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
}
