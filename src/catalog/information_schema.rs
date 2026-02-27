//! Information schema virtual tables.
//!
//! Provides SQL-accessible metadata about tables and columns
//! via the `information_schema` schema.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use super::{CatalogList, TableProvider, TableType};
use crate::error::Result;
use crate::types::{DataType, Field, Schema};

/// Virtual table providing metadata about all tables in the catalog.
#[derive(Debug)]
pub struct InformationSchemaTables {
    schema: Schema,
    catalog_list: Arc<CatalogList>,
}

impl InformationSchemaTables {
    pub fn new(catalog_list: Arc<CatalogList>) -> Self {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);
        Self {
            schema,
            catalog_list,
        }
    }
}

fn table_type_str(tt: TableType) -> &'static str {
    match tt {
        TableType::Base => "BASE TABLE",
        TableType::View => "VIEW",
        TableType::External => "EXTERNAL",
        TableType::Temporary => "TEMPORARY",
    }
}

impl TableProvider for InformationSchemaTables {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let mut catalogs = Vec::new();
        let mut schemas = Vec::new();
        let mut names = Vec::new();
        let mut types = Vec::new();

        for catalog_name in self.catalog_list.catalog_names() {
            if let Some(catalog) = self.catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema_provider) = catalog.schema(&schema_name) {
                        for table_name in schema_provider.table_names() {
                            let tt = schema_provider
                                .table(&table_name)
                                .map(|t| t.table_type())
                                .unwrap_or(TableType::Base);
                            catalogs.push(catalog_name.clone());
                            schemas.push(schema_name.clone());
                            names.push(table_name);
                            types.push(table_type_str(tt).to_string());

                            if let Some(lim) = limit {
                                if catalogs.len() >= lim {
                                    break;
                                }
                            }
                        }
                    }
                    if limit.is_some_and(|l| catalogs.len() >= l) {
                        break;
                    }
                }
            }
            if limit.is_some_and(|l| catalogs.len() >= l) {
                break;
            }
        }

        let arrow_schema = self.schema.to_arrow_ref();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(catalogs)),
                Arc::new(StringArray::from(schemas)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(types)),
            ],
        )
        .map_err(|e| crate::error::BlazeError::execution(e.to_string()))?;

        let batch = if let Some(indices) = projection {
            batch.project(indices).map_err(|e| {
                crate::error::BlazeError::execution(e.to_string())
            })?
        } else {
            batch
        };

        Ok(vec![batch])
    }
}

/// Virtual table providing metadata about all columns in the catalog.
#[derive(Debug)]
pub struct InformationSchemaColumns {
    schema: Schema,
    catalog_list: Arc<CatalogList>,
}

impl InformationSchemaColumns {
    pub fn new(catalog_list: Arc<CatalogList>) -> Self {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int64, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]);
        Self {
            schema,
            catalog_list,
        }
    }
}

impl TableProvider for InformationSchemaColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let mut catalogs = Vec::new();
        let mut schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut column_names = Vec::new();
        let mut ordinal_positions = Vec::new();
        let mut data_types = Vec::new();
        let mut nullables = Vec::new();

        let mut done = false;

        for catalog_name in self.catalog_list.catalog_names() {
            if done {
                break;
            }
            if let Some(catalog) = self.catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if done {
                        break;
                    }
                    if let Some(schema_provider) = catalog.schema(&schema_name) {
                        for tbl_name in schema_provider.table_names() {
                            if done {
                                break;
                            }
                            if let Some(table) = schema_provider.table(&tbl_name) {
                                let tbl_schema = table.schema();
                                for (i, field) in tbl_schema.fields().iter().enumerate() {
                                    catalogs.push(catalog_name.clone());
                                    schemas.push(schema_name.clone());
                                    table_names.push(tbl_name.clone());
                                    column_names.push(field.name().to_string());
                                    ordinal_positions.push((i + 1) as i64);
                                    data_types.push(field.data_type().to_string());
                                    nullables.push(
                                        if field.is_nullable() { "YES" } else { "NO" }.to_string(),
                                    );

                                    if let Some(lim) = limit {
                                        if catalogs.len() >= lim {
                                            done = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let arrow_schema = self.schema.to_arrow_ref();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(catalogs)),
                Arc::new(StringArray::from(schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(column_names)),
                Arc::new(Int64Array::from(ordinal_positions)),
                Arc::new(StringArray::from(data_types)),
                Arc::new(StringArray::from(nullables)),
            ],
        )
        .map_err(|e| crate::error::BlazeError::execution(e.to_string()))?;

        let batch = if let Some(indices) = projection {
            batch.project(indices).map_err(|e| {
                crate::error::BlazeError::execution(e.to_string())
            })?
        } else {
            batch
        };

        Ok(vec![batch])
    }
}

/// Register the `information_schema` schema in the default catalog.
///
/// Must be called after the `CatalogList` is wrapped in `Arc` so
/// the virtual tables can hold a reference back to it.
pub fn register_information_schema(catalog_list: &Arc<CatalogList>) {
    if let Some(catalog) = catalog_list.catalog("default") {
        let info_schema = Arc::new(super::SchemaProvider::new("information_schema"));
        let tables_table: Arc<dyn TableProvider> =
            Arc::new(InformationSchemaTables::new(catalog_list.clone()));
        let columns_table: Arc<dyn TableProvider> =
            Arc::new(InformationSchemaColumns::new(catalog_list.clone()));
        let _ = info_schema.register_table("tables", tables_table);
        let _ = info_schema.register_table("columns", columns_table);
        catalog.register_schema("information_schema", info_schema);
    }
}
