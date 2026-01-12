//! Catalog management for Blaze.
//!
//! The catalog maintains metadata about databases, schemas, tables, and functions.

mod table;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub use table::{Table, TableProvider, TableType, TableStatistics, ColumnStatistics};

use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// A catalog that manages schemas and tables.
#[derive(Debug)]
pub struct Catalog {
    /// Catalog name
    name: String,
    /// Default schema name
    default_schema: RwLock<String>,
    /// Schemas in this catalog
    schemas: RwLock<HashMap<String, Arc<SchemaProvider>>>,
}

impl Catalog {
    /// Create a new catalog with default schema.
    pub fn new() -> Self {
        let catalog = Self {
            name: "default".to_string(),
            default_schema: RwLock::new("main".to_string()),
            schemas: RwLock::new(HashMap::new()),
        };
        // Create default schema
        catalog.schemas.write().insert(
            "main".to_string(),
            Arc::new(SchemaProvider::new("main")),
        );
        catalog
    }

    /// Create a named catalog.
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            default_schema: RwLock::new("main".to_string()),
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Get the catalog name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// List all schema names.
    pub fn schema_names(&self) -> Vec<String> {
        self.schemas.read().keys().cloned().collect()
    }

    /// Get a schema by name.
    pub fn schema(&self, name: &str) -> Option<Arc<SchemaProvider>> {
        self.schemas.read().get(name).cloned()
    }

    /// Register a schema.
    pub fn register_schema(
        &self,
        name: impl Into<String>,
        schema: Arc<SchemaProvider>,
    ) -> Option<Arc<SchemaProvider>> {
        self.schemas.write().insert(name.into(), schema)
    }

    /// Deregister a schema.
    pub fn deregister_schema(&self, name: &str) -> Option<Arc<SchemaProvider>> {
        self.schemas.write().remove(name)
    }

    /// Create a new schema.
    pub fn create_schema(&mut self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write();
        if schemas.contains_key(name) {
            return Err(BlazeError::schema(format!("Schema '{}' already exists", name)));
        }
        schemas.insert(name.to_string(), Arc::new(SchemaProvider::new(name)));
        Ok(())
    }

    /// Set the default schema.
    pub fn set_default_schema(&self, name: &str) {
        *self.default_schema.write() = name.to_string();
    }

    /// Get the default schema name.
    pub fn default_schema(&self) -> String {
        self.default_schema.read().clone()
    }

    /// Register a table in the default schema.
    pub fn register_table(&self, name: &str, table: Arc<dyn TableProvider>) -> Result<()> {
        let default_schema = self.default_schema.read().clone();
        let schemas = self.schemas.read();
        let schema = schemas.get(&default_schema).ok_or_else(|| {
            BlazeError::schema(format!("Default schema '{}' not found", default_schema))
        })?;
        schema.register_table(name, table)?;
        Ok(())
    }

    /// Deregister a table from the default schema.
    pub fn deregister_table(&self, name: &str) -> Result<()> {
        let default_schema = self.default_schema.read().clone();
        let schemas = self.schemas.read();
        let schema = schemas.get(&default_schema).ok_or_else(|| {
            BlazeError::schema(format!("Default schema '{}' not found", default_schema))
        })?;
        schema.deregister_table(name)?;
        Ok(())
    }

    /// List all tables in the default schema.
    pub fn list_tables(&self) -> Vec<String> {
        let default_schema = self.default_schema.read().clone();
        let schemas = self.schemas.read();
        schemas
            .get(&default_schema)
            .map(|s| s.table_names())
            .unwrap_or_default()
    }

    /// Get a table from the default schema.
    pub fn get_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let default_schema = self.default_schema.read().clone();
        let schemas = self.schemas.read();
        schemas.get(&default_schema).and_then(|s| s.table(name))
    }

    /// Check if a table exists in the default schema.
    pub fn table_exists(&self, name: &str) -> bool {
        let default_schema = self.default_schema.read().clone();
        let schemas = self.schemas.read();
        schemas
            .get(&default_schema)
            .map(|s| s.table_exists(name))
            .unwrap_or(false)
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

/// A schema provider that manages tables within a schema.
#[derive(Debug)]
pub struct SchemaProvider {
    /// Schema name
    name: String,
    /// Tables in this schema
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl SchemaProvider {
    /// Create a new schema provider.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Get the schema name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Check if a table exists.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    /// Get a table by name.
    pub fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.read().get(name).cloned()
    }

    /// Register a table.
    pub fn register_table(
        &self,
        name: impl Into<String>,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.write().insert(name.into(), table))
    }

    /// Deregister a table.
    pub fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.write().remove(name))
    }
}

impl Default for SchemaProvider {
    fn default() -> Self {
        Self::new("main")
    }
}

/// The root catalog list that manages multiple catalogs.
#[derive(Debug)]
pub struct CatalogList {
    /// Catalogs
    catalogs: RwLock<HashMap<String, Arc<Catalog>>>,
}

impl CatalogList {
    /// Create a new catalog list.
    pub fn new() -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
        }
    }

    /// List all catalog names.
    pub fn catalog_names(&self) -> Vec<String> {
        self.catalogs.read().keys().cloned().collect()
    }

    /// Get a catalog by name.
    pub fn catalog(&self, name: &str) -> Option<Arc<Catalog>> {
        self.catalogs.read().get(name).cloned()
    }

    /// Register a catalog.
    pub fn register_catalog(
        &self,
        name: impl Into<String>,
        catalog: Arc<Catalog>,
    ) -> Option<Arc<Catalog>> {
        self.catalogs.write().insert(name.into(), catalog)
    }

    /// Deregister a catalog.
    pub fn deregister_catalog(&self, name: &str) -> Option<Arc<Catalog>> {
        self.catalogs.write().remove(name)
    }
}

impl Default for CatalogList {
    fn default() -> Self {
        let list = Self::new();
        // Create default catalog with default schema (Catalog::new() already creates "main" schema)
        let catalog = Arc::new(Catalog::new());
        list.register_catalog("default", catalog);
        list
    }
}

/// Resolved table reference with catalog, schema, and table names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedTableRef {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
}

impl ResolvedTableRef {
    /// Create a new resolved table reference.
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// Create a fully qualified name.
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

impl std::fmt::Display for ResolvedTableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Table reference that may be partially qualified.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableRef {
    /// Optional catalog name
    pub catalog: Option<String>,
    /// Optional schema name
    pub schema: Option<String>,
    /// Table name
    pub table: String,
}

impl TableRef {
    /// Create a table reference from parts.
    pub fn from_parts(parts: &[String]) -> Result<Self> {
        match parts.len() {
            1 => Ok(Self {
                catalog: None,
                schema: None,
                table: parts[0].clone(),
            }),
            2 => Ok(Self {
                catalog: None,
                schema: Some(parts[0].clone()),
                table: parts[1].clone(),
            }),
            3 => Ok(Self {
                catalog: Some(parts[0].clone()),
                schema: Some(parts[1].clone()),
                table: parts[2].clone(),
            }),
            _ => Err(BlazeError::schema(format!(
                "Invalid table reference: {:?}",
                parts
            ))),
        }
    }

    /// Resolve this reference using default catalog and schema.
    pub fn resolve(&self, default_catalog: &str, default_schema: &str) -> ResolvedTableRef {
        ResolvedTableRef {
            catalog: self
                .catalog
                .clone()
                .unwrap_or_else(|| default_catalog.to_string()),
            schema: self
                .schema
                .clone()
                .unwrap_or_else(|| default_schema.to_string()),
            table: self.table.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryTable;

    #[test]
    fn test_catalog_list() {
        let list = CatalogList::default();
        assert!(list.catalog("default").is_some());

        let catalog = list.catalog("default").unwrap();
        assert!(catalog.schema("main").is_some());
    }

    #[test]
    fn test_register_table() {
        let schema_provider = SchemaProvider::new("test");
        let table = Arc::new(MemoryTable::empty(Schema::empty()));
        schema_provider.register_table("my_table", table).unwrap();

        assert!(schema_provider.table_exists("my_table"));
        assert!(schema_provider.table("my_table").is_some());
    }

    #[test]
    fn test_table_ref_resolution() {
        let ref1 = TableRef::from_parts(&["users".to_string()]).unwrap();
        let resolved = ref1.resolve("default", "main");
        assert_eq!(resolved.catalog, "default");
        assert_eq!(resolved.schema, "main");
        assert_eq!(resolved.table, "users");

        let ref2 = TableRef::from_parts(&["public".to_string(), "users".to_string()]).unwrap();
        let resolved = ref2.resolve("default", "main");
        assert_eq!(resolved.schema, "public");
    }
}
