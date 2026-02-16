//! Embedded REST API Server for Blaze.
//!
//! Provides a lightweight, framework-agnostic REST API layer that can be
//! integrated with any HTTP server. Includes routing, request/response models,
//! API key authentication, and rate limiting.
//!
//! # Example
//! ```rust
//! use blaze::rest::{ApiServer, ApiConfig, ApiRequest, HttpMethod};
//!
//! let config = ApiConfig::default();
//! let server = ApiServer::new(config);
//!
//! let request = ApiRequest {
//!     method: HttpMethod::Post,
//!     path: "/api/v1/query".into(),
//!     headers: std::collections::HashMap::new(),
//!     body: Some(r#"{"sql": "SELECT 1"}"#.into()),
//! };
//!
//! let response = server.handle(request);
//! assert_eq!(response.status, 200);
//! ```

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// HTTP primitives
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Options,
}

impl HttpMethod {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "GET" => HttpMethod::Get,
            "POST" => HttpMethod::Post,
            "PUT" => HttpMethod::Put,
            "DELETE" => HttpMethod::Delete,
            "OPTIONS" => HttpMethod::Options,
            _ => HttpMethod::Get,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub method: HttpMethod,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ApiResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

impl ApiResponse {
    pub fn ok(body: impl Into<String>) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".into(), "application/json".into());
        Self {
            status: 200,
            headers,
            body: body.into(),
        }
    }

    pub fn error(status: u16, message: impl Into<String>) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".into(), "application/json".into());
        Self {
            status,
            headers,
            body: format!(r#"{{"error": "{}"}}"#, message.into()),
        }
    }

    pub fn not_found() -> Self {
        Self::error(404, "Not found")
    }

    pub fn unauthorized() -> Self {
        Self::error(401, "Unauthorized")
    }

    pub fn rate_limited() -> Self {
        Self::error(429, "Rate limit exceeded")
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// API keys allowed for authentication. If empty, auth is disabled.
    pub api_keys: Vec<String>,
    /// Maximum requests per minute per API key (0 = unlimited).
    pub rate_limit_per_minute: u32,
    /// Whether to include CORS headers.
    pub enable_cors: bool,
    /// Maximum query result rows returned.
    pub max_result_rows: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            api_keys: Vec::new(),
            rate_limit_per_minute: 0,
            enable_cors: true,
            max_result_rows: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Rate limiter
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RateLimitEntry {
    timestamps: Vec<Instant>,
}

#[derive(Debug)]
pub struct RateLimiter {
    limit_per_minute: u32,
    entries: RwLock<HashMap<String, RateLimitEntry>>,
}

impl RateLimiter {
    pub fn new(limit_per_minute: u32) -> Self {
        Self {
            limit_per_minute,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Check if a request from `key` is allowed. Returns true if allowed.
    pub fn check(&self, key: &str) -> bool {
        if self.limit_per_minute == 0 {
            return true;
        }
        let now = Instant::now();
        let window = Duration::from_secs(60);
        let mut entries = self.entries.write().unwrap();
        let entry = entries
            .entry(key.to_string())
            .or_insert_with(|| RateLimitEntry {
                timestamps: Vec::new(),
            });

        // Remove timestamps outside the window
        entry.timestamps.retain(|t| now.duration_since(*t) < window);

        if entry.timestamps.len() >= self.limit_per_minute as usize {
            return false;
        }
        entry.timestamps.push(now);
        true
    }

    /// Current request count for a key in the current window.
    pub fn current_count(&self, key: &str) -> usize {
        let now = Instant::now();
        let window = Duration::from_secs(60);
        let entries = self.entries.read().unwrap();
        entries
            .get(key)
            .map(|e| {
                e.timestamps
                    .iter()
                    .filter(|t| now.duration_since(**t) < window)
                    .count()
            })
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Route matching
// ---------------------------------------------------------------------------

type RouteHandler = Arc<dyn Fn(&ApiRequest, &ApiServer) -> ApiResponse + Send + Sync>;

struct Route {
    method: HttpMethod,
    path: String,
    handler: RouteHandler,
}

impl std::fmt::Debug for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Route")
            .field("method", &self.method)
            .field("path", &self.path)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// API Server
// ---------------------------------------------------------------------------

pub struct ApiServer {
    pub config: ApiConfig,
    routes: Vec<Route>,
    rate_limiter: RateLimiter,
    /// Statistics
    stats: RwLock<ApiStats>,
}

#[derive(Debug, Clone, Default)]
pub struct ApiStats {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub unauthorized_requests: u64,
    pub rate_limited_requests: u64,
}

impl ApiServer {
    pub fn new(config: ApiConfig) -> Self {
        let rate_limiter = RateLimiter::new(config.rate_limit_per_minute);
        let mut server = Self {
            config,
            routes: Vec::new(),
            rate_limiter,
            stats: RwLock::new(ApiStats::default()),
        };
        server.register_default_routes();
        server
    }

    /// Handle an incoming request through the API pipeline.
    pub fn handle(&self, request: ApiRequest) -> ApiResponse {
        self.stats.write().unwrap().total_requests += 1;

        // Authentication
        if !self.config.api_keys.is_empty() {
            let api_key = request
                .headers
                .get("x-api-key")
                .or_else(|| request.headers.get("X-Api-Key"))
                .or_else(|| request.headers.get("authorization"));
            match api_key {
                Some(key) if self.config.api_keys.contains(key) => {}
                _ => {
                    self.stats.write().unwrap().unauthorized_requests += 1;
                    return ApiResponse::unauthorized();
                }
            }
        }

        // Rate limiting
        let rate_key = request
            .headers
            .get("x-api-key")
            .cloned()
            .unwrap_or_else(|| "anonymous".into());
        if !self.rate_limiter.check(&rate_key) {
            self.stats.write().unwrap().rate_limited_requests += 1;
            return ApiResponse::rate_limited();
        }

        // Route matching
        let mut response = self.dispatch(&request);

        // CORS headers
        if self.config.enable_cors {
            response
                .headers
                .insert("Access-Control-Allow-Origin".into(), "*".into());
            response.headers.insert(
                "Access-Control-Allow-Methods".into(),
                "GET, POST, PUT, DELETE, OPTIONS".into(),
            );
            response.headers.insert(
                "Access-Control-Allow-Headers".into(),
                "Content-Type, X-Api-Key, Authorization".into(),
            );
        }

        if response.status < 400 {
            self.stats.write().unwrap().successful_requests += 1;
        } else {
            self.stats.write().unwrap().failed_requests += 1;
        }
        response
    }

    fn dispatch(&self, request: &ApiRequest) -> ApiResponse {
        // OPTIONS always gets a 200 for CORS preflight
        if request.method == HttpMethod::Options {
            return ApiResponse::ok("{}");
        }

        for route in &self.routes {
            if route.method == request.method && self.path_matches(&route.path, &request.path) {
                return (route.handler)(request, self);
            }
        }
        ApiResponse::not_found()
    }

    fn path_matches(&self, pattern: &str, path: &str) -> bool {
        pattern == path
    }

    fn register_default_routes(&mut self) {
        // Health check
        self.routes.push(Route {
            method: HttpMethod::Get,
            path: "/api/v1/health".into(),
            handler: Arc::new(|_req, _server| {
                ApiResponse::ok(r#"{"status": "ok", "engine": "blaze"}"#)
            }),
        });

        // Query endpoint
        self.routes.push(Route {
            method: HttpMethod::Post,
            path: "/api/v1/query".into(),
            handler: Arc::new(|req, server| {
                let body = match &req.body {
                    Some(b) => b,
                    None => return ApiResponse::error(400, "Missing request body"),
                };

                // Simple JSON parsing: extract "sql" field
                let sql = extract_json_string(body, "sql");
                match sql {
                    Some(sql) => {
                        let max_rows = server.config.max_result_rows;
                        ApiResponse::ok(format!(
                            r#"{{"sql": "{}", "max_rows": {}, "status": "accepted"}}"#,
                            sql, max_rows
                        ))
                    }
                    None => ApiResponse::error(400, "Missing 'sql' field in request body"),
                }
            }),
        });

        // Stats endpoint
        self.routes.push(Route {
            method: HttpMethod::Get,
            path: "/api/v1/stats".into(),
            handler: Arc::new(|_req, server| {
                let stats = server.stats.read().unwrap().clone();
                ApiResponse::ok(format!(
                    r#"{{"total_requests": {}, "successful": {}, "failed": {}, "unauthorized": {}, "rate_limited": {}}}"#,
                    stats.total_requests,
                    stats.successful_requests,
                    stats.failed_requests,
                    stats.unauthorized_requests,
                    stats.rate_limited_requests,
                ))
            }),
        });

        // Version endpoint
        self.routes.push(Route {
            method: HttpMethod::Get,
            path: "/api/v1/version".into(),
            handler: Arc::new(|_req, _server| {
                ApiResponse::ok(format!(
                    r#"{{"version": "{}", "name": "blaze"}}"#,
                    env!("CARGO_PKG_VERSION")
                ))
            }),
        });
    }

    /// Add a custom route.
    pub fn add_route(
        &mut self,
        method: HttpMethod,
        path: impl Into<String>,
        handler: impl Fn(&ApiRequest, &ApiServer) -> ApiResponse + Send + Sync + 'static,
    ) {
        self.routes.push(Route {
            method,
            path: path.into(),
            handler: Arc::new(handler),
        });
    }

    /// Get current server statistics.
    pub fn stats(&self) -> ApiStats {
        self.stats.read().unwrap().clone()
    }

    /// List all registered routes.
    pub fn routes(&self) -> Vec<(String, String)> {
        self.routes
            .iter()
            .map(|r| (format!("{:?}", r.method), r.path.clone()))
            .collect()
    }
}

/// Simple JSON string field extractor (avoids serde dependency).
fn extract_json_string(json: &str, key: &str) -> Option<String> {
    let pattern = format!(r#""{}""#, key);
    let idx = json.find(&pattern)?;
    let rest = &json[idx + pattern.len()..];
    // Skip `: "`
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?;
    let rest = rest.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_server() -> ApiServer {
        ApiServer::new(ApiConfig::default())
    }

    fn make_request(method: HttpMethod, path: &str) -> ApiRequest {
        ApiRequest {
            method,
            path: path.into(),
            headers: HashMap::new(),
            body: None,
        }
    }

    #[test]
    fn test_health_endpoint() {
        let server = make_server();
        let req = make_request(HttpMethod::Get, "/api/v1/health");
        let resp = server.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("ok"));
    }

    #[test]
    fn test_version_endpoint() {
        let server = make_server();
        let req = make_request(HttpMethod::Get, "/api/v1/version");
        let resp = server.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("blaze"));
    }

    #[test]
    fn test_query_endpoint() {
        let server = make_server();
        let req = ApiRequest {
            method: HttpMethod::Post,
            path: "/api/v1/query".into(),
            headers: HashMap::new(),
            body: Some(r#"{"sql": "SELECT 1"}"#.into()),
        };
        let resp = server.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("SELECT 1"));
    }

    #[test]
    fn test_query_missing_sql() {
        let server = make_server();
        let req = ApiRequest {
            method: HttpMethod::Post,
            path: "/api/v1/query".into(),
            headers: HashMap::new(),
            body: Some(r#"{"foo": "bar"}"#.into()),
        };
        let resp = server.handle(req);
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn test_not_found() {
        let server = make_server();
        let req = make_request(HttpMethod::Get, "/nonexistent");
        let resp = server.handle(req);
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn test_cors_headers() {
        let server = make_server();
        let req = make_request(HttpMethod::Get, "/api/v1/health");
        let resp = server.handle(req);
        assert!(resp.headers.contains_key("Access-Control-Allow-Origin"));
    }

    #[test]
    fn test_options_preflight() {
        let server = make_server();
        let req = make_request(HttpMethod::Options, "/api/v1/query");
        let resp = server.handle(req);
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_api_key_auth() {
        let config = ApiConfig {
            api_keys: vec!["secret-key-123".into()],
            ..Default::default()
        };
        let server = ApiServer::new(config);

        // No key → 401
        let req = make_request(HttpMethod::Get, "/api/v1/health");
        let resp = server.handle(req);
        assert_eq!(resp.status, 401);

        // Wrong key → 401
        let mut req = make_request(HttpMethod::Get, "/api/v1/health");
        req.headers.insert("x-api-key".into(), "wrong".into());
        let resp = server.handle(req);
        assert_eq!(resp.status, 401);

        // Correct key → 200
        let mut req = make_request(HttpMethod::Get, "/api/v1/health");
        req.headers
            .insert("x-api-key".into(), "secret-key-123".into());
        let resp = server.handle(req);
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_rate_limiting() {
        let config = ApiConfig {
            rate_limit_per_minute: 3,
            ..Default::default()
        };
        let server = ApiServer::new(config);

        for i in 0..5 {
            let req = make_request(HttpMethod::Get, "/api/v1/health");
            let resp = server.handle(req);
            if i < 3 {
                assert_eq!(resp.status, 200, "Request {} should succeed", i);
            } else {
                assert_eq!(resp.status, 429, "Request {} should be rate limited", i);
            }
        }
    }

    #[test]
    fn test_stats_tracking() {
        let server = make_server();
        let req = make_request(HttpMethod::Get, "/api/v1/health");
        server.handle(req);
        let req = make_request(HttpMethod::Get, "/nonexistent");
        server.handle(req);

        let stats = server.stats();
        assert_eq!(stats.total_requests, 2);
        assert_eq!(stats.successful_requests, 1);
        assert_eq!(stats.failed_requests, 1);
    }

    #[test]
    fn test_custom_route() {
        let mut server = make_server();
        server.add_route(HttpMethod::Get, "/api/v1/custom", |_req, _server| {
            ApiResponse::ok(r#"{"custom": true}"#)
        });

        let req = make_request(HttpMethod::Get, "/api/v1/custom");
        let resp = server.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("custom"));
    }

    #[test]
    fn test_list_routes() {
        let server = make_server();
        let routes = server.routes();
        assert!(routes.len() >= 4); // health, query, stats, version
    }

    #[test]
    fn test_rate_limiter_standalone() {
        let limiter = RateLimiter::new(2);
        assert!(limiter.check("key1"));
        assert!(limiter.check("key1"));
        assert!(!limiter.check("key1"));
        // Different key still works
        assert!(limiter.check("key2"));
        assert_eq!(limiter.current_count("key1"), 2);
    }

    #[test]
    fn test_extract_json_string() {
        assert_eq!(
            extract_json_string(r#"{"sql": "SELECT 1"}"#, "sql"),
            Some("SELECT 1".into())
        );
        assert_eq!(extract_json_string(r#"{"foo": "bar"}"#, "sql"), None);
    }

    #[test]
    fn test_http_method_from_str() {
        assert_eq!(HttpMethod::from_str("GET"), HttpMethod::Get);
        assert_eq!(HttpMethod::from_str("post"), HttpMethod::Post);
        assert_eq!(HttpMethod::from_str("DELETE"), HttpMethod::Delete);
    }

    #[test]
    fn test_api_response_helpers() {
        let ok = ApiResponse::ok("test");
        assert_eq!(ok.status, 200);
        assert_eq!(ok.body, "test");

        let err = ApiResponse::error(500, "fail");
        assert_eq!(err.status, 500);
        assert!(err.body.contains("fail"));
    }
}
