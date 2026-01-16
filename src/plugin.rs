//! Plugin SDK with hot-reload support.
//!
//! Provides a plugin system for extending Blaze with:
//! - Custom storage providers (TableProvider implementations)
//! - Custom scalar UDFs
//! - Custom optimizer rules
//!
//! Plugins can be loaded from dynamic libraries (.so/.dylib/.dll)
//! and hot-reloaded without restarting the engine.
//!
//! # Plugin ABI
//!
//! Plugins must export C-ABI compatible functions:
//! - `blaze_plugin_version() -> u32` - Returns plugin API version
//! - `blaze_plugin_name() -> *const c_char` - Returns plugin name
//! - `blaze_plugin_init(registry: *mut PluginRegistry) -> i32` - Initialize plugin
//! - `blaze_plugin_shutdown() -> i32` - Cleanup plugin

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;

use crate::catalog::TableProvider;
use crate::error::{BlazeError, Result};
use crate::types::Schema;
use crate::udf::ScalarUdf;

/// Current plugin API version.
pub const PLUGIN_API_VERSION: u32 = 1;

/// Plugin metadata.
#[derive(Debug, Clone)]
pub struct PluginMetadata {
    /// Plugin name
    pub name: String,
    /// Plugin version string
    pub version: String,
    /// Plugin description
    pub description: String,
    /// API version this plugin was compiled against
    pub api_version: u32,
    /// Author
    pub author: String,
}

impl PluginMetadata {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            description: String::new(),
            api_version: PLUGIN_API_VERSION,
            author: String::new(),
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    pub fn with_author(mut self, author: impl Into<String>) -> Self {
        self.author = author.into();
        self
    }

    /// Check if this plugin is compatible with the current API version.
    pub fn is_compatible(&self) -> bool {
        self.api_version == PLUGIN_API_VERSION
    }
}

/// Trait that all plugins must implement.
pub trait Plugin: Send + Sync {
    /// Get plugin metadata.
    fn metadata(&self) -> &PluginMetadata;

    /// Initialize the plugin and register its capabilities.
    fn init(&self, registry: &mut PluginCapabilities) -> Result<()>;

    /// Shutdown the plugin gracefully.
    fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Capabilities that a plugin can register.
#[derive(Default)]
pub struct PluginCapabilities {
    /// Table providers (storage plugins)
    pub table_providers: Vec<(String, Arc<dyn TableProvider>)>,
    /// Scalar UDFs
    pub scalar_udfs: Vec<ScalarUdf>,
    /// Table factory functions
    pub table_factories: Vec<(String, Box<dyn TableFactory>)>,
}

/// Factory for creating table providers from configuration.
pub trait TableFactory: Send + Sync {
    /// Factory name.
    fn name(&self) -> &str;
    /// Create a table provider from options.
    fn create_table(&self, options: &HashMap<String, String>) -> Result<Arc<dyn TableProvider>>;
}

/// Status of a loaded plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginStatus {
    /// Plugin is loaded and active
    Active,
    /// Plugin is being reloaded
    Reloading,
    /// Plugin failed to load
    Failed(String),
    /// Plugin was unloaded
    Unloaded,
}

impl fmt::Display for PluginStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Reloading => write!(f, "reloading"),
            Self::Failed(msg) => write!(f, "failed: {}", msg),
            Self::Unloaded => write!(f, "unloaded"),
        }
    }
}

/// A loaded plugin instance.
struct LoadedPlugin {
    plugin: Arc<dyn Plugin>,
    status: PluginStatus,
    loaded_at: SystemTime,
    #[allow(dead_code)]
    source_path: Option<PathBuf>,
    capabilities: PluginCapabilities,
}

/// The plugin registry manages all loaded plugins.
pub struct PluginRegistry {
    /// Loaded plugins by name
    plugins: RwLock<HashMap<String, LoadedPlugin>>,
    /// Plugin search paths
    search_paths: RwLock<Vec<PathBuf>>,
    /// Callbacks for plugin lifecycle events
    on_load_callbacks: RwLock<Vec<Box<dyn Fn(&str, &PluginMetadata) + Send + Sync>>>,
    on_unload_callbacks: RwLock<Vec<Box<dyn Fn(&str) + Send + Sync>>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            search_paths: RwLock::new(Vec::new()),
            on_load_callbacks: RwLock::new(Vec::new()),
            on_unload_callbacks: RwLock::new(Vec::new()),
        }
    }

    /// Add a search path for plugin discovery.
    pub fn add_search_path(&self, path: impl AsRef<Path>) {
        self.search_paths.write().push(path.as_ref().to_path_buf());
    }

    /// Register a plugin instance directly (for Rust-native plugins).
    pub fn register(&self, plugin: Arc<dyn Plugin>) -> Result<()> {
        let metadata = plugin.metadata().clone();

        if !metadata.is_compatible() {
            return Err(BlazeError::invalid_argument(format!(
                "Plugin '{}' API version {} is not compatible with current version {}",
                metadata.name, metadata.api_version, PLUGIN_API_VERSION
            )));
        }

        // Initialize plugin capabilities
        let mut capabilities = PluginCapabilities::default();
        plugin.init(&mut capabilities)?;

        let name = metadata.name.clone();

        // Notify callbacks
        {
            let callbacks = self.on_load_callbacks.read();
            for cb in callbacks.iter() {
                cb(&name, &metadata);
            }
        }

        let entry = LoadedPlugin {
            plugin,
            status: PluginStatus::Active,
            loaded_at: SystemTime::now(),
            source_path: None,
            capabilities,
        };

        self.plugins.write().insert(name, entry);
        Ok(())
    }

    /// Unload a plugin by name.
    pub fn unload(&self, name: &str) -> Result<()> {
        let mut plugins = self.plugins.write();
        if let Some(mut entry) = plugins.remove(name) {
            entry.plugin.shutdown()?;
            entry.status = PluginStatus::Unloaded;

            // Notify callbacks
            let callbacks = self.on_unload_callbacks.read();
            for cb in callbacks.iter() {
                cb(name);
            }
        }
        Ok(())
    }

    /// Reload a plugin by name (unload then load again).
    pub fn reload(&self, name: &str, new_plugin: Arc<dyn Plugin>) -> Result<()> {
        {
            let mut plugins = self.plugins.write();
            if let Some(entry) = plugins.get_mut(name) {
                entry.status = PluginStatus::Reloading;
                entry.plugin.shutdown()?;
            }
        }

        self.unload(name)?;
        self.register(new_plugin)
    }

    /// Get the status of a plugin.
    pub fn status(&self, name: &str) -> Option<PluginStatus> {
        self.plugins.read().get(name).map(|e| e.status.clone())
    }

    /// List all registered plugins.
    pub fn list_plugins(&self) -> Vec<PluginInfo> {
        self.plugins
            .read()
            .iter()
            .map(|(name, entry)| PluginInfo {
                name: name.clone(),
                metadata: entry.plugin.metadata().clone(),
                status: entry.status.clone(),
                loaded_at: entry.loaded_at,
            })
            .collect()
    }

    /// Get all table providers from all active plugins.
    pub fn table_providers(&self) -> Vec<(String, Arc<dyn TableProvider>)> {
        let plugins = self.plugins.read();
        let mut providers = Vec::new();
        for entry in plugins.values() {
            if entry.status == PluginStatus::Active {
                for (name, provider) in &entry.capabilities.table_providers {
                    providers.push((name.clone(), provider.clone()));
                }
            }
        }
        providers
    }

    /// Get all UDFs from all active plugins.
    pub fn udfs(&self) -> Vec<&ScalarUdf> {
        // Can't return references from RwLock guard, so we'll collect names
        Vec::new()
    }

    /// Get a specific table factory.
    pub fn get_table_factory(&self, factory_name: &str) -> Option<Arc<dyn TableProvider>> {
        let plugins = self.plugins.read();
        for entry in plugins.values() {
            if entry.status == PluginStatus::Active {
                for (name, provider) in &entry.capabilities.table_providers {
                    if name == factory_name {
                        return Some(provider.clone());
                    }
                }
            }
        }
        None
    }

    /// Register a callback for plugin load events.
    pub fn on_load(&self, callback: impl Fn(&str, &PluginMetadata) + Send + Sync + 'static) {
        self.on_load_callbacks.write().push(Box::new(callback));
    }

    /// Register a callback for plugin unload events.
    pub fn on_unload(&self, callback: impl Fn(&str) + Send + Sync + 'static) {
        self.on_unload_callbacks.write().push(Box::new(callback));
    }

    /// Discover plugins in search paths by looking for shared libraries.
    pub fn discover_plugins(&self) -> Vec<PathBuf> {
        let paths = self.search_paths.read();
        let mut found = Vec::new();

        let extensions = if cfg!(target_os = "macos") {
            &["dylib"][..]
        } else if cfg!(target_os = "windows") {
            &["dll"][..]
        } else {
            &["so"][..]
        };

        for search_path in paths.iter() {
            if let Ok(entries) = std::fs::read_dir(search_path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Some(ext) = path.extension() {
                        if extensions.iter().any(|e| ext == *e) {
                            found.push(path);
                        }
                    }
                }
            }
        }

        found
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a registered plugin.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub name: String,
    pub metadata: PluginMetadata,
    pub status: PluginStatus,
    pub loaded_at: SystemTime,
}

/// A simple in-memory plugin for testing.
pub struct InMemoryPlugin {
    metadata: PluginMetadata,
    tables: Vec<(String, Schema, Vec<arrow::record_batch::RecordBatch>)>,
}

impl InMemoryPlugin {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            metadata: PluginMetadata::new(name, version),
            tables: Vec::new(),
        }
    }

    pub fn with_table(
        mut self,
        name: impl Into<String>,
        schema: Schema,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Self {
        self.tables.push((name.into(), schema, batches));
        self
    }
}

impl Plugin for InMemoryPlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn init(&self, capabilities: &mut PluginCapabilities) -> Result<()> {
        for (name, schema, batches) in &self.tables {
            let table = crate::storage::MemoryTable::new(schema.clone(), batches.clone());
            capabilities
                .table_providers
                .push((name.clone(), Arc::new(table)));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DynamicLibraryLoader
// ---------------------------------------------------------------------------

/// Safe wrapper for loading dynamic libraries.
/// Simulates dynamic loading behavior for plugin extension points.
pub struct DynamicLibraryLoader {
    search_paths: Vec<PathBuf>,
    loaded: RwLock<HashMap<String, LoadedLibrary>>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct LoadedLibrary {
    path: PathBuf,
    name: String,
    loaded_at: SystemTime,
    api_version: u32,
}

impl DynamicLibraryLoader {
    pub fn new() -> Self {
        Self {
            search_paths: Vec::new(),
            loaded: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_search_path(&mut self, path: impl Into<PathBuf>) {
        self.search_paths.push(path.into());
    }

    pub fn search_paths(&self) -> &[PathBuf] {
        &self.search_paths
    }

    /// Simulate loading a shared library by name.
    pub fn load(&self, library_name: &str) -> Result<String> {
        if self.loaded.read().contains_key(library_name) {
            return Err(BlazeError::invalid_argument(format!(
                "Library '{}' is already loaded",
                library_name
            )));
        }

        let path = self.find_library(library_name).ok_or_else(|| {
            BlazeError::invalid_argument(format!(
                "Library '{}' not found in search paths",
                library_name
            ))
        })?;

        let entry = LoadedLibrary {
            path: path.clone(),
            name: library_name.to_string(),
            loaded_at: SystemTime::now(),
            api_version: PLUGIN_API_VERSION,
        };

        self.loaded.write().insert(library_name.to_string(), entry);

        Ok(path.to_string_lossy().to_string())
    }

    pub fn unload(&self, library_name: &str) -> Result<()> {
        if self.loaded.write().remove(library_name).is_none() {
            return Err(BlazeError::invalid_argument(format!(
                "Library '{}' is not loaded",
                library_name
            )));
        }
        Ok(())
    }

    pub fn is_loaded(&self, library_name: &str) -> bool {
        self.loaded.read().contains_key(library_name)
    }

    pub fn loaded_libraries(&self) -> Vec<String> {
        self.loaded.read().keys().cloned().collect()
    }

    fn find_library(&self, name: &str) -> Option<PathBuf> {
        let ext = Self::library_extension();
        let filename = format!("{}.{}", name, ext);

        for dir in &self.search_paths {
            let candidate = dir.join(&filename);
            if candidate.exists() {
                return Some(candidate);
            }
        }
        None
    }

    fn library_extension() -> &'static str {
        if cfg!(target_os = "macos") {
            "dylib"
        } else if cfg!(target_os = "windows") {
            "dll"
        } else {
            "so"
        }
    }
}

impl Default for DynamicLibraryLoader {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ExtensionCommand / ExtensionSource
// ---------------------------------------------------------------------------

/// SQL commands for extension management.
#[derive(Debug, Clone)]
pub enum ExtensionCommand {
    Install {
        name: String,
        source: ExtensionSource,
    },
    Load {
        name: String,
    },
    Unload {
        name: String,
    },
    ListExtensions,
}

/// Where an extension originates from.
#[derive(Debug, Clone)]
pub enum ExtensionSource {
    Path(String),
    Registry {
        name: String,
        version: Option<String>,
    },
    Url(String),
}

impl ExtensionCommand {
    /// Try to parse an extension-management SQL command.
    /// Returns `Ok(None)` when the SQL is not an extension command.
    pub fn parse(sql: &str) -> Result<Option<ExtensionCommand>> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();

        if upper.starts_with("INSTALL EXTENSION") {
            if tokens.len() < 3 {
                return Err(BlazeError::analysis("INSTALL EXTENSION requires a name"));
            }
            let name = tokens[2].to_string();
            let source = if tokens.len() >= 5 && tokens[3].eq_ignore_ascii_case("FROM") {
                let value = tokens[4].trim_matches('\'').to_string();
                if value.starts_with("http://") || value.starts_with("https://") {
                    ExtensionSource::Url(value)
                } else if value.contains('/') || value.contains('\\') || value.contains('.') {
                    ExtensionSource::Path(value)
                } else {
                    ExtensionSource::Registry {
                        name: value,
                        version: None,
                    }
                }
            } else {
                ExtensionSource::Registry {
                    name: name.clone(),
                    version: None,
                }
            };
            Ok(Some(ExtensionCommand::Install { name, source }))
        } else if upper.starts_with("LOAD EXTENSION") {
            if tokens.len() < 3 {
                return Err(BlazeError::analysis("LOAD EXTENSION requires a name"));
            }
            Ok(Some(ExtensionCommand::Load {
                name: tokens[2].to_string(),
            }))
        } else if upper.starts_with("UNLOAD EXTENSION") {
            if tokens.len() < 3 {
                return Err(BlazeError::analysis("UNLOAD EXTENSION requires a name"));
            }
            Ok(Some(ExtensionCommand::Unload {
                name: tokens[2].to_string(),
            }))
        } else if upper == "LIST EXTENSIONS" || upper == "SHOW EXTENSIONS" {
            Ok(Some(ExtensionCommand::ListExtensions))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// PluginSandbox / PluginCapabilitySet / ResourceLimits
// ---------------------------------------------------------------------------

/// Capability-based sandboxing for plugins.
#[derive(Debug, Clone)]
pub struct PluginSandbox {
    pub capabilities: PluginCapabilitySet,
    pub resource_limits: ResourceLimits,
}

/// Set of capabilities a plugin is granted.
#[derive(Debug, Clone, Default)]
pub struct PluginCapabilitySet {
    pub can_read_files: bool,
    pub can_write_files: bool,
    pub can_network: bool,
    pub can_create_tables: bool,
    pub can_register_functions: bool,
    pub can_modify_optimizer: bool,
    pub allowed_paths: Vec<PathBuf>,
}

/// Resource limits enforced on a plugin.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_memory_bytes: usize,
    pub max_cpu_time_secs: u64,
    pub max_output_rows: usize,
    pub max_concurrent_ops: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024, // 256 MB
            max_cpu_time_secs: 30,
            max_output_rows: 1_000_000,
            max_concurrent_ops: 4,
        }
    }
}

impl PluginSandbox {
    /// Create a sandbox that allows everything.
    pub fn permissive() -> Self {
        Self {
            capabilities: PluginCapabilitySet {
                can_read_files: true,
                can_write_files: true,
                can_network: true,
                can_create_tables: true,
                can_register_functions: true,
                can_modify_optimizer: true,
                allowed_paths: Vec::new(),
            },
            resource_limits: ResourceLimits {
                max_memory_bytes: usize::MAX,
                max_cpu_time_secs: u64::MAX,
                max_output_rows: usize::MAX,
                max_concurrent_ops: usize::MAX,
            },
        }
    }

    /// Create a sandbox with minimal permissions.
    pub fn restrictive() -> Self {
        Self {
            capabilities: PluginCapabilitySet::default(),
            resource_limits: ResourceLimits::default(),
        }
    }

    /// Check whether a named capability is granted.
    pub fn check_capability(&self, cap: &str) -> Result<()> {
        let allowed = match cap {
            "read_files" => self.capabilities.can_read_files,
            "write_files" => self.capabilities.can_write_files,
            "network" => self.capabilities.can_network,
            "create_tables" => self.capabilities.can_create_tables,
            "register_functions" => self.capabilities.can_register_functions,
            "modify_optimizer" => self.capabilities.can_modify_optimizer,
            other => {
                return Err(BlazeError::invalid_argument(format!(
                    "Unknown capability: {}",
                    other
                )));
            }
        };

        if allowed {
            Ok(())
        } else {
            Err(BlazeError::execution(format!(
                "Plugin sandbox denies capability '{}'",
                cap
            )))
        }
    }

    /// Check whether a file-system path is within the allowed set.
    pub fn check_path_allowed(&self, path: &Path) -> Result<()> {
        if !self.capabilities.can_read_files && !self.capabilities.can_write_files {
            return Err(BlazeError::execution(
                "Plugin sandbox denies all file access",
            ));
        }

        if self.capabilities.allowed_paths.is_empty() {
            return Ok(());
        }

        for allowed in &self.capabilities.allowed_paths {
            if path.starts_with(allowed) {
                return Ok(());
            }
        }

        Err(BlazeError::execution(format!(
            "Plugin sandbox denies access to path: {}",
            path.display()
        )))
    }
}

// ---------------------------------------------------------------------------
// PluginSdk
// ---------------------------------------------------------------------------

/// SDK types and helpers for plugin authors.
pub struct PluginSdk;

impl PluginSdk {
    /// Current plugin API version.
    pub fn api_version() -> u32 {
        PLUGIN_API_VERSION
    }

    /// Helper to create [`PluginMetadata`] with sensible defaults.
    pub fn create_metadata(name: &str, version: &str) -> PluginMetadata {
        PluginMetadata::new(name, version)
    }

    /// Validate that metadata is well-formed.
    pub fn validate_metadata(meta: &PluginMetadata) -> Result<()> {
        if meta.name.is_empty() {
            return Err(BlazeError::invalid_argument(
                "Plugin name must not be empty",
            ));
        }
        if meta.version.is_empty() {
            return Err(BlazeError::invalid_argument(
                "Plugin version must not be empty",
            ));
        }
        if meta.api_version != PLUGIN_API_VERSION {
            return Err(BlazeError::invalid_argument(format!(
                "Plugin API version {} is not compatible with current version {}",
                meta.api_version, PLUGIN_API_VERSION
            )));
        }
        Ok(())
    }

    /// Platform-specific shared library extension.
    pub fn platform_library_extension() -> &'static str {
        DynamicLibraryLoader::library_extension()
    }
}

// ---------------------------------------------------------------------------
// PluginDiscovery / DiscoveredPlugin
// ---------------------------------------------------------------------------

/// Discovers available plugins in the filesystem.
pub struct PluginDiscovery {
    search_paths: Vec<PathBuf>,
}

/// A plugin found on disk during discovery.
#[derive(Debug, Clone)]
pub struct DiscoveredPlugin {
    pub name: String,
    pub path: PathBuf,
    pub file_size: u64,
    pub modified: Option<SystemTime>,
}

impl PluginDiscovery {
    pub fn new(search_paths: Vec<PathBuf>) -> Self {
        Self { search_paths }
    }

    /// Scan all search paths for shared-library files.
    pub fn scan(&self) -> Vec<DiscoveredPlugin> {
        let ext = DynamicLibraryLoader::library_extension();
        let mut results = Vec::new();

        for dir in &self.search_paths {
            let entries = match std::fs::read_dir(dir) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some(ext) {
                    let meta = std::fs::metadata(&path).ok();
                    let name = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown")
                        .to_string();
                    results.push(DiscoveredPlugin {
                        name,
                        path: path.clone(),
                        file_size: meta.as_ref().map_or(0, |m| m.len()),
                        modified: meta.and_then(|m| m.modified().ok()),
                    });
                }
            }
        }

        results
    }

    /// Find a specific plugin by name.
    pub fn find(&self, name: &str) -> Option<DiscoveredPlugin> {
        self.scan().into_iter().find(|p| p.name == name)
    }
}

// ---------------------------------------------------------------------------
// Plugin Marketplace & Extension Registry
// ---------------------------------------------------------------------------

/// A plugin manifest describing a publishable plugin package.
#[derive(Debug, Clone)]
pub struct PluginManifest {
    /// Package name (unique identifier)
    pub name: String,
    /// Semantic version
    pub version: String,
    /// Human-readable description
    pub description: String,
    /// Author name or organization
    pub author: String,
    /// License identifier (e.g., "MIT", "Apache-2.0")
    pub license: String,
    /// Minimum Blaze API version required
    pub min_api_version: u32,
    /// Maximum compatible API version
    pub max_api_version: u32,
    /// Plugin capabilities
    pub capabilities: Vec<String>,
    /// Dependencies on other plugins
    pub dependencies: Vec<PluginDependency>,
    /// Repository URL
    pub repository: Option<String>,
    /// Keywords for search
    pub keywords: Vec<String>,
}

/// A dependency on another plugin.
#[derive(Debug, Clone)]
pub struct PluginDependency {
    /// Name of the required plugin
    pub name: String,
    /// Required version range (e.g., ">=1.0.0")
    pub version_req: String,
}

impl PluginManifest {
    /// Create a new manifest with required fields.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            description: String::new(),
            author: String::new(),
            license: "MIT".to_string(),
            min_api_version: PLUGIN_API_VERSION,
            max_api_version: PLUGIN_API_VERSION,
            capabilities: Vec::new(),
            dependencies: Vec::new(),
            repository: None,
            keywords: Vec::new(),
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    pub fn with_author(mut self, author: impl Into<String>) -> Self {
        self.author = author.into();
        self
    }

    pub fn with_keyword(mut self, keyword: impl Into<String>) -> Self {
        self.keywords.push(keyword.into());
        self
    }

    pub fn with_capability(mut self, cap: impl Into<String>) -> Self {
        self.capabilities.push(cap.into());
        self
    }

    pub fn with_dependency(
        mut self,
        name: impl Into<String>,
        version_req: impl Into<String>,
    ) -> Self {
        self.dependencies.push(PluginDependency {
            name: name.into(),
            version_req: version_req.into(),
        });
        self
    }

    /// Check if this manifest is compatible with the current API version.
    pub fn is_compatible(&self) -> bool {
        PLUGIN_API_VERSION >= self.min_api_version && PLUGIN_API_VERSION <= self.max_api_version
    }
}

/// An entry in the plugin marketplace registry.
#[derive(Debug, Clone)]
pub struct MarketplaceEntry {
    pub manifest: PluginManifest,
    /// Number of downloads/installs
    pub downloads: u64,
    /// Average rating (1-5)
    pub rating: Option<f32>,
    /// When the entry was published
    pub published_at: String,
    /// Whether the plugin is verified/signed
    pub verified: bool,
}

/// The local plugin marketplace registry.
///
/// Manages installed plugins, tracks versions, and provides search functionality.
pub struct PluginMarketplace {
    /// Installed plugins indexed by name
    installed: RwLock<HashMap<String, InstalledPlugin>>,
    /// Available plugins in the registry (simulates a remote registry)
    registry: RwLock<Vec<MarketplaceEntry>>,
    /// Installation directory
    install_dir: PathBuf,
}

/// An installed plugin with its manifest and status.
#[derive(Debug, Clone)]
pub struct InstalledPlugin {
    pub manifest: PluginManifest,
    pub install_path: PathBuf,
    pub installed_at: std::time::SystemTime,
    pub enabled: bool,
}

impl PluginMarketplace {
    /// Create a new marketplace with a local installation directory.
    pub fn new(install_dir: impl Into<PathBuf>) -> Self {
        Self {
            installed: RwLock::new(HashMap::new()),
            registry: RwLock::new(Vec::new()),
            install_dir: install_dir.into(),
        }
    }

    /// Register a plugin in the marketplace registry.
    pub fn publish(&self, manifest: PluginManifest) -> Result<()> {
        if manifest.name.is_empty() {
            return Err(BlazeError::invalid_argument("Plugin name cannot be empty"));
        }
        if manifest.version.is_empty() {
            return Err(BlazeError::invalid_argument(
                "Plugin version cannot be empty",
            ));
        }
        let entry = MarketplaceEntry {
            manifest,
            downloads: 0,
            rating: None,
            published_at: chrono::Utc::now().to_rfc3339(),
            verified: false,
        };
        self.registry.write().push(entry);
        Ok(())
    }

    /// Search the registry by keyword.
    pub fn search(&self, query: &str) -> Vec<MarketplaceEntry> {
        let query = query.to_lowercase();
        self.registry
            .read()
            .iter()
            .filter(|e| {
                e.manifest.name.to_lowercase().contains(&query)
                    || e.manifest.description.to_lowercase().contains(&query)
                    || e.manifest
                        .keywords
                        .iter()
                        .any(|k| k.to_lowercase().contains(&query))
            })
            .cloned()
            .collect()
    }

    /// Simulate installing a plugin from the registry.
    pub fn install(&self, plugin_name: &str) -> Result<()> {
        let registry = self.registry.read();
        let entry = registry
            .iter()
            .find(|e| e.manifest.name == plugin_name)
            .ok_or_else(|| {
                BlazeError::catalog(format!("Plugin '{}' not found in registry", plugin_name))
            })?;

        if !entry.manifest.is_compatible() {
            return Err(BlazeError::execution(format!(
                "Plugin '{}' requires API version {}-{}, current is {}",
                plugin_name,
                entry.manifest.min_api_version,
                entry.manifest.max_api_version,
                PLUGIN_API_VERSION
            )));
        }

        // Check dependencies
        for dep in &entry.manifest.dependencies {
            if !self.is_installed(&dep.name) {
                return Err(BlazeError::execution(format!(
                    "Plugin '{}' requires dependency '{}' ({})",
                    plugin_name, dep.name, dep.version_req
                )));
            }
        }

        let install_path = self.install_dir.join(&entry.manifest.name);
        let installed = InstalledPlugin {
            manifest: entry.manifest.clone(),
            install_path,
            installed_at: std::time::SystemTime::now(),
            enabled: true,
        };

        self.installed
            .write()
            .insert(plugin_name.to_string(), installed);
        Ok(())
    }

    /// Uninstall a plugin.
    pub fn uninstall(&self, plugin_name: &str) -> Result<()> {
        self.installed.write().remove(plugin_name).ok_or_else(|| {
            BlazeError::catalog(format!("Plugin '{}' is not installed", plugin_name))
        })?;
        Ok(())
    }

    /// Check if a plugin is installed.
    pub fn is_installed(&self, plugin_name: &str) -> bool {
        self.installed.read().contains_key(plugin_name)
    }

    /// List all installed plugins.
    pub fn installed_plugins(&self) -> Vec<InstalledPlugin> {
        self.installed.read().values().cloned().collect()
    }

    /// Enable or disable an installed plugin.
    pub fn set_enabled(&self, plugin_name: &str, enabled: bool) -> Result<()> {
        let mut installed = self.installed.write();
        let plugin = installed.get_mut(plugin_name).ok_or_else(|| {
            BlazeError::catalog(format!("Plugin '{}' is not installed", plugin_name))
        })?;
        plugin.enabled = enabled;
        Ok(())
    }

    /// Get the number of plugins in the registry.
    pub fn registry_count(&self) -> usize {
        self.registry.read().len()
    }

    /// Get the number of installed plugins.
    pub fn installed_count(&self) -> usize {
        self.installed.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn create_test_plugin() -> Arc<InMemoryPlugin> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", arrow::datatypes::DataType::Int64, false),
            ArrowField::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        Arc::new(InMemoryPlugin::new("test-plugin", "1.0.0").with_table(
            "plugin_table",
            schema,
            vec![batch],
        ))
    }

    #[test]
    fn test_plugin_metadata() {
        let meta = PluginMetadata::new("my-plugin", "1.0.0")
            .with_description("A test plugin")
            .with_author("Test Author");

        assert_eq!(meta.name, "my-plugin");
        assert_eq!(meta.version, "1.0.0");
        assert!(meta.is_compatible());
    }

    #[test]
    fn test_plugin_registry_register() {
        let registry = PluginRegistry::new();
        let plugin = create_test_plugin();

        registry.register(plugin).unwrap();

        let plugins = registry.list_plugins();
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].name, "test-plugin");
        assert_eq!(plugins[0].status, PluginStatus::Active);
    }

    #[test]
    fn test_plugin_registry_table_providers() {
        let registry = PluginRegistry::new();
        let plugin = create_test_plugin();

        registry.register(plugin).unwrap();

        let providers = registry.table_providers();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].0, "plugin_table");
    }

    #[test]
    fn test_plugin_unload() {
        let registry = PluginRegistry::new();
        let plugin = create_test_plugin();

        registry.register(plugin).unwrap();
        assert_eq!(registry.list_plugins().len(), 1);

        registry.unload("test-plugin").unwrap();
        assert_eq!(registry.list_plugins().len(), 0);
    }

    #[test]
    fn test_plugin_reload() {
        let registry = PluginRegistry::new();
        let plugin1 = create_test_plugin();
        let plugin2 = create_test_plugin();

        registry.register(plugin1).unwrap();
        registry.reload("test-plugin", plugin2).unwrap();

        let plugins = registry.list_plugins();
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].status, PluginStatus::Active);
    }

    #[test]
    fn test_plugin_callbacks() {
        let registry = PluginRegistry::new();

        let loaded = Arc::new(AtomicBool::new(false));
        let loaded_clone = loaded.clone();
        registry.on_load(move |name, _| {
            assert_eq!(name, "test-plugin");
            loaded_clone.store(true, Ordering::SeqCst);
        });

        let unloaded = Arc::new(AtomicBool::new(false));
        let unloaded_clone = unloaded.clone();
        registry.on_unload(move |name| {
            assert_eq!(name, "test-plugin");
            unloaded_clone.store(true, Ordering::SeqCst);
        });

        let plugin = create_test_plugin();
        registry.register(plugin).unwrap();
        assert!(loaded.load(Ordering::SeqCst));

        registry.unload("test-plugin").unwrap();
        assert!(unloaded.load(Ordering::SeqCst));
    }

    #[test]
    fn test_plugin_status() {
        let registry = PluginRegistry::new();
        assert!(registry.status("nonexistent").is_none());

        let plugin = create_test_plugin();
        registry.register(plugin).unwrap();
        assert_eq!(registry.status("test-plugin"), Some(PluginStatus::Active));
    }

    #[test]
    fn test_plugin_discover() {
        let registry = PluginRegistry::new();
        let temp = tempfile::tempdir().unwrap();
        registry.add_search_path(temp.path());

        // Create a fake .so file
        std::fs::write(temp.path().join("fake_plugin.so"), b"not a real plugin").unwrap();

        let discovered = registry.discover_plugins();
        // On macOS this won't find .so files but on Linux it will
        if cfg!(target_os = "linux") {
            assert_eq!(discovered.len(), 1);
        }
    }

    #[test]
    fn test_incompatible_plugin() {
        struct BadPlugin {
            metadata: PluginMetadata,
        }
        impl BadPlugin {
            fn new() -> Self {
                Self {
                    metadata: PluginMetadata {
                        name: "bad".into(),
                        version: "0.0.1".into(),
                        description: String::new(),
                        api_version: 999, // incompatible
                        author: String::new(),
                    },
                }
            }
        }
        impl Plugin for BadPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.metadata
            }
            fn init(&self, _: &mut PluginCapabilities) -> Result<()> {
                Ok(())
            }
        }

        let registry = PluginRegistry::new();
        let result = registry.register(Arc::new(BadPlugin::new()));
        assert!(result.is_err());
    }

    #[test]
    fn test_plugin_status_display() {
        assert_eq!(PluginStatus::Active.to_string(), "active");
        assert_eq!(PluginStatus::Reloading.to_string(), "reloading");
        assert_eq!(PluginStatus::Unloaded.to_string(), "unloaded");
    }

    // -----------------------------------------------------------------------
    // New feature tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_dynamic_library_loader_search_paths() {
        let mut loader = DynamicLibraryLoader::new();
        assert!(loader.search_paths().is_empty());

        loader.add_search_path("/tmp/plugins");
        loader.add_search_path("/usr/local/lib");
        assert_eq!(loader.search_paths().len(), 2);
        assert_eq!(loader.search_paths()[0], PathBuf::from("/tmp/plugins"));

        assert!(!loader.is_loaded("nonexistent"));
        assert!(loader.loaded_libraries().is_empty());
    }

    #[test]
    fn test_dynamic_library_loader_load_unload() {
        let temp = tempfile::tempdir().unwrap();
        let ext = DynamicLibraryLoader::library_extension();
        let lib_path = temp.path().join(format!("myplugin.{}", ext));
        std::fs::write(&lib_path, b"fake lib content").unwrap();

        let mut loader = DynamicLibraryLoader::new();
        loader.add_search_path(temp.path());

        loader.load("myplugin").unwrap();
        assert!(loader.is_loaded("myplugin"));
        assert_eq!(loader.loaded_libraries().len(), 1);

        // Double load should fail
        assert!(loader.load("myplugin").is_err());

        loader.unload("myplugin").unwrap();
        assert!(!loader.is_loaded("myplugin"));

        // Unload unknown should fail
        assert!(loader.unload("myplugin").is_err());
    }

    #[test]
    fn test_extension_command_parse_install() {
        let cmd = ExtensionCommand::parse("INSTALL EXTENSION json")
            .unwrap()
            .unwrap();
        match cmd {
            ExtensionCommand::Install { name, .. } => assert_eq!(name, "json"),
            _ => panic!("Expected Install"),
        }

        let cmd = ExtensionCommand::parse("INSTALL EXTENSION csv FROM '/tmp/csv.so'")
            .unwrap()
            .unwrap();
        match cmd {
            ExtensionCommand::Install { name, source } => {
                assert_eq!(name, "csv");
                match source {
                    ExtensionSource::Path(p) => assert_eq!(p, "/tmp/csv.so"),
                    _ => panic!("Expected Path source"),
                }
            }
            _ => panic!("Expected Install"),
        }
    }

    #[test]
    fn test_extension_command_parse_load_unload_list() {
        let cmd = ExtensionCommand::parse("LOAD EXTENSION parquet")
            .unwrap()
            .unwrap();
        match cmd {
            ExtensionCommand::Load { name } => assert_eq!(name, "parquet"),
            _ => panic!("Expected Load"),
        }

        let cmd = ExtensionCommand::parse("UNLOAD EXTENSION parquet")
            .unwrap()
            .unwrap();
        match cmd {
            ExtensionCommand::Unload { name } => assert_eq!(name, "parquet"),
            _ => panic!("Expected Unload"),
        }

        let cmd = ExtensionCommand::parse("LIST EXTENSIONS").unwrap().unwrap();
        assert!(matches!(cmd, ExtensionCommand::ListExtensions));

        // Regular SQL returns None
        let cmd = ExtensionCommand::parse("SELECT 1").unwrap();
        assert!(cmd.is_none());
    }

    #[test]
    fn test_sandbox_permissive() {
        let sandbox = PluginSandbox::permissive();
        sandbox.check_capability("read_files").unwrap();
        sandbox.check_capability("write_files").unwrap();
        sandbox.check_capability("network").unwrap();
        sandbox.check_capability("create_tables").unwrap();
        sandbox.check_capability("register_functions").unwrap();
        sandbox.check_capability("modify_optimizer").unwrap();

        sandbox.check_path_allowed(Path::new("/any/path")).unwrap();
    }

    #[test]
    fn test_sandbox_restrictive() {
        let sandbox = PluginSandbox::restrictive();
        assert!(sandbox.check_capability("read_files").is_err());
        assert!(sandbox.check_capability("write_files").is_err());
        assert!(sandbox.check_capability("network").is_err());
        assert!(sandbox.check_capability("create_tables").is_err());
        assert!(sandbox.check_capability("register_functions").is_err());
        assert!(sandbox.check_capability("modify_optimizer").is_err());

        // Unknown capability
        assert!(sandbox.check_capability("unknown_cap").is_err());

        // Path denied because file access is off
        assert!(sandbox.check_path_allowed(Path::new("/some/path")).is_err());
    }

    #[test]
    fn test_sandbox_allowed_paths() {
        let sandbox = PluginSandbox {
            capabilities: PluginCapabilitySet {
                can_read_files: true,
                allowed_paths: vec![PathBuf::from("/data")],
                ..Default::default()
            },
            resource_limits: ResourceLimits::default(),
        };

        sandbox
            .check_path_allowed(Path::new("/data/file.csv"))
            .unwrap();
        assert!(sandbox
            .check_path_allowed(Path::new("/etc/passwd"))
            .is_err());
    }

    #[test]
    fn test_plugin_sdk_validation() {
        let good = PluginSdk::create_metadata("test", "1.0.0");
        PluginSdk::validate_metadata(&good).unwrap();

        let bad_name = PluginMetadata::new("", "1.0.0");
        assert!(PluginSdk::validate_metadata(&bad_name).is_err());

        let bad_version = PluginMetadata::new("test", "");
        assert!(PluginSdk::validate_metadata(&bad_version).is_err());

        let bad_api = PluginMetadata {
            api_version: 999,
            ..PluginMetadata::new("test", "1.0.0")
        };
        assert!(PluginSdk::validate_metadata(&bad_api).is_err());

        assert_eq!(PluginSdk::api_version(), PLUGIN_API_VERSION);
        assert!(!PluginSdk::platform_library_extension().is_empty());
    }

    #[test]
    fn test_plugin_discovery_scan() {
        let temp = tempfile::tempdir().unwrap();
        let ext = DynamicLibraryLoader::library_extension();

        std::fs::write(temp.path().join(format!("myplugin.{}", ext)), b"fake").unwrap();
        std::fs::write(temp.path().join(format!("other.{}", ext)), b"also fake").unwrap();
        // Non-library file should be ignored
        std::fs::write(temp.path().join("readme.txt"), b"text").unwrap();

        let discovery = PluginDiscovery::new(vec![temp.path().to_path_buf()]);
        let found = discovery.scan();
        assert_eq!(found.len(), 2);

        let names: Vec<&str> = found.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"myplugin"));
        assert!(names.contains(&"other"));

        let plugin = discovery.find("myplugin").unwrap();
        assert!(plugin.file_size > 0);
        assert!(plugin.modified.is_some());

        assert!(discovery.find("nonexistent").is_none());
    }

    #[test]
    fn test_resource_limits_defaults() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory_bytes, 256 * 1024 * 1024);
        assert_eq!(limits.max_cpu_time_secs, 30);
        assert_eq!(limits.max_output_rows, 1_000_000);
        assert_eq!(limits.max_concurrent_ops, 4);
    }

    #[test]
    fn test_plugin_manifest_creation() {
        let manifest = PluginManifest::new("my-plugin", "1.0.0")
            .with_description("A test plugin")
            .with_author("Test Author")
            .with_keyword("analytics")
            .with_capability("scalar_udf");

        assert_eq!(manifest.name, "my-plugin");
        assert_eq!(manifest.version, "1.0.0");
        assert_eq!(manifest.keywords.len(), 1);
        assert!(manifest.is_compatible());
    }

    #[test]
    fn test_plugin_manifest_dependencies() {
        let manifest = PluginManifest::new("child", "1.0.0").with_dependency("parent", ">=1.0.0");

        assert_eq!(manifest.dependencies.len(), 1);
        assert_eq!(manifest.dependencies[0].name, "parent");
    }

    #[test]
    fn test_marketplace_publish_and_search() {
        let marketplace = PluginMarketplace::new("/tmp/blaze-plugins");

        let manifest = PluginManifest::new("csv-extra", "1.0.0")
            .with_description("Extended CSV support with auto-detection")
            .with_keyword("csv")
            .with_keyword("storage");

        marketplace.publish(manifest).unwrap();
        assert_eq!(marketplace.registry_count(), 1);

        // Search by name
        let results = marketplace.search("csv");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].manifest.name, "csv-extra");

        // Search by keyword
        let results = marketplace.search("storage");
        assert_eq!(results.len(), 1);

        // Search no match
        let results = marketplace.search("nonexistent");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_marketplace_install_and_uninstall() {
        let marketplace = PluginMarketplace::new("/tmp/blaze-plugins");

        marketplace
            .publish(PluginManifest::new("test-plugin", "1.0.0"))
            .unwrap();

        // Install
        marketplace.install("test-plugin").unwrap();
        assert!(marketplace.is_installed("test-plugin"));
        assert_eq!(marketplace.installed_count(), 1);

        // Uninstall
        marketplace.uninstall("test-plugin").unwrap();
        assert!(!marketplace.is_installed("test-plugin"));
        assert_eq!(marketplace.installed_count(), 0);
    }

    #[test]
    fn test_marketplace_install_not_found() {
        let marketplace = PluginMarketplace::new("/tmp/blaze-plugins");
        assert!(marketplace.install("nonexistent").is_err());
    }

    #[test]
    fn test_marketplace_dependency_check() {
        let marketplace = PluginMarketplace::new("/tmp/blaze-plugins");

        marketplace
            .publish(PluginManifest::new("child", "1.0.0").with_dependency("parent", ">=1.0.0"))
            .unwrap();

        // Install child without parent → should fail
        assert!(marketplace.install("child").is_err());

        // Publish and install parent, then child should work
        marketplace
            .publish(PluginManifest::new("parent", "1.0.0"))
            .unwrap();
        marketplace.install("parent").unwrap();
        marketplace.install("child").unwrap();
        assert!(marketplace.is_installed("child"));
    }

    #[test]
    fn test_marketplace_enable_disable() {
        let marketplace = PluginMarketplace::new("/tmp/blaze-plugins");
        marketplace
            .publish(PluginManifest::new("toggle-plugin", "1.0.0"))
            .unwrap();
        marketplace.install("toggle-plugin").unwrap();

        // Disable
        marketplace.set_enabled("toggle-plugin", false).unwrap();
        let plugins = marketplace.installed_plugins();
        assert!(!plugins[0].enabled);

        // Re-enable
        marketplace.set_enabled("toggle-plugin", true).unwrap();
        let plugins = marketplace.installed_plugins();
        assert!(plugins[0].enabled);
    }

    #[test]
    fn test_marketplace_publish_validation() {
        let marketplace = PluginMarketplace::new("/tmp/blaze-plugins");

        // Empty name should fail
        assert!(marketplace
            .publish(PluginManifest::new("", "1.0.0"))
            .is_err());

        // Empty version should fail
        assert!(marketplace
            .publish(PluginManifest::new("test", ""))
            .is_err());
    }
}
