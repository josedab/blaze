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

use subtle::ConstantTimeEq;

use crate::error::{BlazeError, Result};

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

    pub fn payload_too_large() -> Self {
        Self::error(413, "Request body too large")
    }
}

// ---------------------------------------------------------------------------
// JWT authentication
// ---------------------------------------------------------------------------

/// JWT authentication configuration.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// HMAC secret for JWT validation (HS256).
    pub secret: String,
    /// Expected issuer (optional).
    pub issuer: Option<String>,
    /// Expected audience (optional).
    pub audience: Option<String>,
    /// Whether JWT auth is enabled.
    pub enabled: bool,
}

impl JwtConfig {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
            issuer: None,
            audience: None,
            enabled: true,
        }
    }

    pub fn with_issuer(mut self, issuer: &str) -> Self {
        self.issuer = Some(issuer.to_string());
        self
    }

    pub fn with_audience(mut self, audience: &str) -> Self {
        self.audience = Some(audience.to_string());
        self
    }

    /// Validate a JWT token (basic HS256 HMAC validation).
    pub fn validate_token(&self, token: &str) -> std::result::Result<JwtClaims, String> {
        // Split token into parts
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err("Invalid JWT format".to_string());
        }

        // Verify HMAC-SHA256 signature
        use base64::Engine;
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;

        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let mac = hmac_sha256(self.secret.as_bytes(), signing_input.as_bytes());
        let expected_sig = engine.encode(&mac);

        // Constant-time comparison
        if !constant_time_eq(expected_sig.as_bytes(), parts[2].as_bytes()) {
            return Err("Invalid signature".to_string());
        }

        // Decode claims
        let claims_json = engine
            .decode(parts[1])
            .map_err(|e| format!("Invalid base64: {}", e))?;
        let claims: JwtClaims = serde_json::from_slice(&claims_json)
            .map_err(|e| format!("Invalid claims JSON: {}", e))?;

        // Check expiry
        if let Some(exp) = claims.exp {
            let now = chrono::Utc::now().timestamp() as u64;
            if now > exp {
                return Err("Token expired".to_string());
            }
        }

        Ok(claims)
    }
}

/// Decoded JWT claims.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct JwtClaims {
    pub sub: Option<String>,
    pub exp: Option<u64>,
    pub iss: Option<String>,
    pub aud: Option<String>,
}

/// Simple HMAC-SHA256 using the `sha2` crate.
fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};

    let block_size = 64;
    let mut padded_key = vec![0u8; block_size];
    if key.len() > block_size {
        let hash = Sha256::digest(key);
        padded_key[..hash.len()].copy_from_slice(&hash);
    } else {
        padded_key[..key.len()].copy_from_slice(key);
    }

    let ipad: Vec<u8> = padded_key.iter().map(|b| b ^ 0x36).collect();
    let opad: Vec<u8> = padded_key.iter().map(|b| b ^ 0x5c).collect();

    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(inner_hash);
    outer.finalize().to_vec()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
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
    /// Allowed CORS origin (e.g. "https://example.com"). Defaults to None (no origin header).
    pub cors_allowed_origin: Option<String>,
    /// Maximum query result rows returned.
    pub max_result_rows: usize,
    /// Maximum request body size in bytes (0 = unlimited).
    pub max_body_size: usize,
    /// Path to TLS certificate file (PEM format).
    pub tls_cert_path: Option<String>,
    /// Path to TLS private key file (PEM format).
    pub tls_key_path: Option<String>,
    /// JWT authentication configuration.
    pub jwt_config: Option<JwtConfig>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            api_keys: Vec::new(),
            rate_limit_per_minute: 0,
            enable_cors: true,
            cors_allowed_origin: None,
            max_result_rows: 10_000,
            max_body_size: 0,
            tls_cert_path: None,
            tls_key_path: None,
            jwt_config: None,
        }
    }
}

impl ApiConfig {
    /// Configure TLS with certificate and key file paths.
    ///
    /// NOTE: This sets the TLS configuration only. Actual TLS socket binding
    /// requires a runtime dependency (e.g. tokio-rustls or native-tls) which
    /// should be added separately.
    pub fn with_tls(mut self, cert_path: &str, key_path: &str) -> Self {
        self.tls_cert_path = Some(cert_path.to_string());
        self.tls_key_path = Some(key_path.to_string());
        self
    }

    /// Configure JWT authentication.
    pub fn with_jwt(mut self, config: JwtConfig) -> Self {
        self.jwt_config = Some(config);
        self
    }

    /// Returns `true` if both TLS certificate and key paths are configured.
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }

    /// Validate that configured TLS certificate and key files exist on disk.
    pub fn validate_tls(&self) -> Result<()> {
        if let (Some(cert), Some(key)) = (&self.tls_cert_path, &self.tls_key_path) {
            if !std::path::Path::new(cert).exists() {
                return Err(BlazeError::invalid_argument(format!(
                    "TLS certificate file not found: {}",
                    cert
                )));
            }
            if !std::path::Path::new(key).exists() {
                return Err(BlazeError::invalid_argument(format!(
                    "TLS key file not found: {}",
                    key
                )));
            }
        }
        Ok(())
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
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
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

        // Prune empty entries to prevent unbounded HashMap growth (DoS vector)
        const MAX_ENTRIES: usize = 100_000;
        if entries.len() > MAX_ENTRIES {
            entries.retain(|_, e| !e.timestamps.is_empty());
        }

        true
    }

    /// Current request count for a key in the current window.
    pub fn current_count(&self, key: &str) -> usize {
        let now = Instant::now();
        let window = Duration::from_secs(60);
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
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
        self.stats
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .total_requests += 1;

        // Body size limit
        if self.config.max_body_size > 0 {
            if let Some(ref body) = request.body {
                if body.len() > self.config.max_body_size {
                    return ApiResponse::payload_too_large();
                }
            }
        }

        // API key authentication
        if !self.config.api_keys.is_empty() {
            let api_key = request
                .headers
                .get("x-api-key")
                .or_else(|| request.headers.get("X-Api-Key"))
                .or_else(|| request.headers.get("authorization"));
            match api_key {
                Some(key)
                    if self
                        .config
                        .api_keys
                        .iter()
                        .any(|k| k.as_bytes().ct_eq(key.as_bytes()).into()) => {}
                _ => {
                    self.stats
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                        .unauthorized_requests += 1;
                    return ApiResponse::unauthorized();
                }
            }
        }

        // JWT authentication
        if let Some(ref jwt) = self.config.jwt_config {
            if jwt.enabled {
                let token = request
                    .headers
                    .get("authorization")
                    .or_else(|| request.headers.get("Authorization"))
                    .and_then(|v| {
                        v.strip_prefix("Bearer ")
                            .or_else(|| v.strip_prefix("bearer "))
                    });
                match token {
                    Some(t) => {
                        if jwt.validate_token(t).is_err() {
                            self.stats
                                .write()
                                .unwrap_or_else(|e| e.into_inner())
                                .unauthorized_requests += 1;
                            return ApiResponse::unauthorized();
                        }
                    }
                    None => {
                        self.stats
                            .write()
                            .unwrap_or_else(|e| e.into_inner())
                            .unauthorized_requests += 1;
                        return ApiResponse::unauthorized();
                    }
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
            self.stats
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .rate_limited_requests += 1;
            return ApiResponse::rate_limited();
        }

        // Route matching
        let mut response = self.dispatch(&request);

        // CORS headers
        if self.config.enable_cors {
            if let Some(origin) = self.config.cors_allowed_origin.as_deref() {
                response
                    .headers
                    .insert("Access-Control-Allow-Origin".into(), origin.into());
            }
            response.headers.insert(
                "Access-Control-Allow-Methods".into(),
                "GET, POST, PUT, DELETE, OPTIONS".into(),
            );
            response.headers.insert(
                "Access-Control-Allow-Headers".into(),
                "Content-Type, X-Api-Key, Authorization".into(),
            );
        }

        // Security headers
        response
            .headers
            .insert("X-Content-Type-Options".into(), "nosniff".into());
        response
            .headers
            .insert("X-Frame-Options".into(), "DENY".into());
        response.headers.insert(
            "Content-Security-Policy".into(),
            "default-src 'none'; frame-ancestors 'none'".into(),
        );

        if response.status < 400 {
            self.stats
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .successful_requests += 1;
        } else {
            self.stats
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .failed_requests += 1;
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
                    Some(_sql) => {
                        let max_rows = server.config.max_result_rows;
                        ApiResponse::ok(format!(
                            r#"{{"max_rows": {}, "status": "accepted"}}"#,
                            max_rows
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
                let stats = server.stats.read().unwrap_or_else(|e| e.into_inner()).clone();
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
        self.stats.read().unwrap_or_else(|e| e.into_inner()).clone()
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
        assert!(resp.body.contains("accepted"));
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
        let config = ApiConfig {
            cors_allowed_origin: Some("https://example.com".into()),
            ..ApiConfig::default()
        };
        let server = ApiServer::new(config);
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
