//! Schema definitions for Blaze.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};

use super::DataType;
use crate::error::{BlazeError, Result};

/// A field in a schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// Field name
    name: String,
    /// Field data type
    data_type: DataType,
    /// Whether the field can contain nulls
    nullable: bool,
    /// Optional metadata
    metadata: HashMap<String, String>,
}

impl Field {
    /// Create a new field.
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }

    /// Create a new non-nullable field.
    pub fn new_non_null(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, false)
    }

    /// Create a new nullable field.
    pub fn new_nullable(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, true)
    }

    /// Get the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the data type.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Check if the field is nullable.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Get the metadata.
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Set metadata for this field.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Add a metadata entry.
    pub fn with_metadata_entry(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Convert to Arrow field.
    pub fn to_arrow(&self) -> ArrowField {
        let mut field = ArrowField::new(&self.name, self.data_type.to_arrow(), self.nullable);
        if !self.metadata.is_empty() {
            field.set_metadata(self.metadata.clone());
        }
        field
    }

    /// Convert from Arrow field.
    pub fn from_arrow(arrow_field: &ArrowField) -> Result<Self> {
        Ok(Self {
            name: arrow_field.name().clone(),
            data_type: DataType::from_arrow(arrow_field.data_type())?,
            nullable: arrow_field.is_nullable(),
            metadata: arrow_field.metadata().clone(),
        })
    }

    /// Create a qualified field name (table.column format).
    pub fn qualified_name(&self, qualifier: Option<&str>) -> String {
        match qualifier {
            Some(q) => format!("{}.{}", q, self.name),
            None => self.name.clone(),
        }
    }
}

/// A schema consisting of multiple fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// The fields in this schema
    fields: Vec<Field>,
    /// Map from field name to index for fast lookup
    field_index: HashMap<String, usize>,
    /// Optional metadata
    metadata: HashMap<String, String>,
}

impl Schema {
    /// Create a new empty schema.
    pub fn empty() -> Self {
        Self {
            fields: Vec::new(),
            field_index: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    /// Create a new schema from fields.
    pub fn new(fields: Vec<Field>) -> Self {
        let field_index = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), i))
            .collect();
        Self {
            fields,
            field_index,
            metadata: HashMap::new(),
        }
    }

    /// Create a schema with metadata.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Get the fields in this schema.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Get a field by index.
    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    /// Get a field by name.
    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.field_index.get(name).map(|&i| &self.fields[i])
    }

    /// Get the index of a field by name.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.field_index.get(name).copied()
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if the schema is empty.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Check if a field exists.
    pub fn contains(&self, name: &str) -> bool {
        self.field_index.contains_key(name)
    }

    /// Get the metadata.
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Project the schema to a subset of fields.
    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        let fields: Result<Vec<Field>> = indices
            .iter()
            .map(|&i| {
                self.fields
                    .get(i)
                    .cloned()
                    .ok_or_else(|| BlazeError::schema(format!("Invalid field index: {}", i)))
            })
            .collect();
        Ok(Self::new(fields?))
    }

    /// Merge two schemas.
    pub fn merge(&self, other: &Schema) -> Self {
        let mut fields = self.fields.clone();
        fields.extend(other.fields.iter().cloned());
        Self::new(fields)
    }

    /// Convert to Arrow schema.
    pub fn to_arrow(&self) -> ArrowSchema {
        let arrow_fields: Vec<ArrowField> = self.fields.iter().map(|f| f.to_arrow()).collect();
        if self.metadata.is_empty() {
            ArrowSchema::new(arrow_fields)
        } else {
            ArrowSchema::new_with_metadata(arrow_fields, self.metadata.clone())
        }
    }

    /// Convert to Arrow schema reference.
    pub fn to_arrow_ref(&self) -> Arc<ArrowSchema> {
        Arc::new(self.to_arrow())
    }

    /// Convert from Arrow schema.
    pub fn from_arrow(arrow_schema: &ArrowSchema) -> Result<Self> {
        let fields: Result<Vec<Field>> = arrow_schema
            .fields()
            .iter()
            .map(|f| Field::from_arrow(f))
            .collect();
        Ok(Self::new(fields?).with_metadata(arrow_schema.metadata().clone()))
    }

    /// Get an iterator over field names.
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name())
    }

    /// Get an iterator over data types.
    pub fn data_types(&self) -> impl Iterator<Item = &DataType> {
        self.fields.iter().map(|f| f.data_type())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<Field>> for Schema {
    fn from(fields: Vec<Field>) -> Self {
        Self::new(fields)
    }
}

impl FromIterator<Field> for Schema {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

/// A reference to a schema, potentially qualified with a table name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedSchema {
    /// Optional qualifier (table name, alias, etc.)
    pub qualifier: Option<String>,
    /// The schema
    pub schema: Schema,
}

impl QualifiedSchema {
    /// Create a new qualified schema.
    pub fn new(qualifier: Option<String>, schema: Schema) -> Self {
        Self { qualifier, schema }
    }

    /// Create an unqualified schema.
    pub fn unqualified(schema: Schema) -> Self {
        Self {
            qualifier: None,
            schema,
        }
    }

    /// Get a qualified field name.
    pub fn qualified_field_name(&self, field_name: &str) -> String {
        match &self.qualifier {
            Some(q) => format!("{}.{}", q, field_name),
            None => field_name.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]);

        assert_eq!(schema.len(), 3);
        assert_eq!(schema.field(0).unwrap().name(), "id");
        assert_eq!(schema.index_of("name"), Some(1));
        assert!(schema.contains("age"));
        assert!(!schema.contains("unknown"));
    }

    #[test]
    fn test_schema_project() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let projected = schema.project(&[0, 2]).unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.field(0).unwrap().name(), "a");
        assert_eq!(projected.field(1).unwrap().name(), "c");
    }

    #[test]
    fn test_arrow_roundtrip() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let arrow_schema = schema.to_arrow();
        let back = Schema::from_arrow(&arrow_schema).unwrap();
        assert_eq!(schema, back);
    }
}
