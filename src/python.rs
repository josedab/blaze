//! Python bindings for Blaze using PyO3.
//!
//! This module provides Python bindings for the Blaze query engine,
//! allowing Python users to execute SQL queries and interact with
//! various data formats.
//!
//! # Example (Python)
//!
//! ```python
//! import pyblaze
//!
//! # Create an in-memory connection
//! conn = pyblaze.connect()
//!
//! # Create a table
//! conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
//!
//! # Insert data
//! conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
//!
//! # Query data
//! result = conn.query("SELECT * FROM users")
//! print(result.to_pyarrow())  # Convert to PyArrow Table
//! ```

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::TableProvider;
use crate::prepared::{PreparedStatement as RustPreparedStatement, PreparedStatementCache};
use crate::{BlazeError, Connection as RustConnection, ConnectionConfig, Result, ScalarValue};
use arrow::record_batch::RecordBatch;

/// Convert a BlazeError to a Python exception.
fn to_py_err(e: BlazeError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Convert a Blaze Result to a Python Result.
fn py_result<T>(result: Result<T>) -> PyResult<T> {
    result.map_err(to_py_err)
}

/// Python wrapper for Blaze Connection.
///
/// Provides the main interface for executing SQL queries against
/// in-memory tables and external files.
#[pyclass(name = "Connection")]
pub struct PyConnection {
    inner: RustConnection,
    stmt_cache: Arc<PreparedStatementCache>,
}

#[pymethods]
impl PyConnection {
    /// Create a new in-memory database connection.
    #[new]
    #[pyo3(signature = (*, batch_size=8192, memory_limit=None))]
    fn new(batch_size: usize, memory_limit: Option<usize>) -> PyResult<Self> {
        let mut config = ConnectionConfig::new().with_batch_size(batch_size);
        if let Some(limit) = memory_limit {
            config = config.with_memory_limit(limit);
        }

        let inner = py_result(RustConnection::with_config(config))?;
        Ok(Self {
            inner,
            stmt_cache: Arc::new(PreparedStatementCache::new(100)),
        })
    }

    /// Execute a SQL query and return the results.
    ///
    /// Args:
    ///     sql: The SQL query to execute.
    ///
    /// Returns:
    ///     A QueryResult containing the query results.
    fn query(&self, sql: &str) -> PyResult<PyQueryResult> {
        let batches = py_result(self.inner.query(sql))?;
        Ok(PyQueryResult { batches })
    }

    /// Execute a SQL statement that doesn't return results.
    ///
    /// This is used for DDL statements like CREATE TABLE, DROP TABLE, etc.
    ///
    /// Args:
    ///     sql: The SQL statement to execute.
    ///
    /// Returns:
    ///     The number of rows affected.
    fn execute(&self, sql: &str) -> PyResult<usize> {
        py_result(self.inner.execute(sql))
    }

    /// Register a CSV file as a table.
    ///
    /// Args:
    ///     name: The name to register the table under.
    ///     path: The path to the CSV file.
    ///     delimiter: Optional delimiter character (default: ',').
    ///     has_header: Whether the file has a header row (default: True).
    #[pyo3(signature = (name, path, *, delimiter=None, has_header=true))]
    fn register_csv(
        &self,
        name: &str,
        path: &str,
        delimiter: Option<char>,
        has_header: bool,
    ) -> PyResult<()> {
        let mut options = crate::storage::CsvOptions::default();
        if let Some(d) = delimiter {
            options.delimiter = d as u8;
        }
        options.has_header = has_header;

        py_result(self.inner.register_csv_with_options(name, path, options))
    }

    /// Register a Parquet file as a table.
    ///
    /// Args:
    ///     name: The name to register the table under.
    ///     path: The path to the Parquet file.
    fn register_parquet(&self, name: &str, path: &str) -> PyResult<()> {
        py_result(self.inner.register_parquet(name, path))
    }

    /// List all registered tables.
    ///
    /// Returns:
    ///     A list of table names.
    fn list_tables(&self) -> Vec<String> {
        self.inner.list_tables()
    }

    /// Get the schema of a table.
    ///
    /// Args:
    ///     name: The name of the table.
    ///
    /// Returns:
    ///     A dictionary describing the schema, or None if the table doesn't exist.
    fn table_schema(&self, name: &str) -> Option<PySchema> {
        self.inner.table_schema(name).map(|s| PySchema { inner: s })
    }

    /// Prepare a SQL statement for repeated execution.
    ///
    /// Args:
    ///     sql: The SQL query with parameter placeholders ($1, $2, etc.).
    ///
    /// Returns:
    ///     A PreparedStatement that can be executed with different parameter values.
    fn prepare(&self, sql: &str) -> PyResult<PyPreparedStatement> {
        let stmt = py_result(self.inner.prepare(sql))?;
        Ok(PyPreparedStatement { inner: stmt })
    }

    /// Prepare a SQL statement using the statement cache.
    ///
    /// This method uses a cache to avoid re-parsing frequently used queries.
    ///
    /// Args:
    ///     sql: The SQL query with parameter placeholders ($1, $2, etc.).
    ///
    /// Returns:
    ///     A PreparedStatement that can be executed with different parameter values.
    fn prepare_cached(&self, sql: &str) -> PyResult<PyPreparedStatement> {
        let stmt = py_result(self.inner.prepare_cached(sql, &self.stmt_cache))?;
        Ok(PyPreparedStatement { inner: stmt })
    }

    /// Create a table from a Python dictionary of lists.
    ///
    /// Args:
    ///     name: The name for the new table.
    ///     data: A dictionary mapping column names to lists of values.
    ///
    /// Example:
    ///     conn.from_dict("users", {"id": [1, 2, 3], "name": ["a", "b", "c"]})
    fn from_dict(&self, py: Python<'_>, name: &str, data: &Bound<'_, PyDict>) -> PyResult<()> {
        let batches = dict_to_record_batches(py, data)?;
        py_result(self.inner.register_batches(name, batches))
    }

    fn __repr__(&self) -> String {
        format!("Connection(tables={})", self.inner.list_tables().len())
    }
}

/// Python wrapper for query results.
#[pyclass(name = "QueryResult")]
pub struct PyQueryResult {
    batches: Vec<RecordBatch>,
}

#[pymethods]
impl PyQueryResult {
    /// Get the number of rows in the result.
    fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Get the number of columns in the result.
    fn num_columns(&self) -> usize {
        self.batches.first().map(|b| b.num_columns()).unwrap_or(0)
    }

    /// Get the column names.
    fn column_names(&self) -> Vec<String> {
        self.batches
            .first()
            .map(|b| {
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Convert to a list of dictionaries (row-oriented).
    ///
    /// Returns:
    ///     A list of dictionaries, where each dictionary represents a row.
    fn to_dicts(&self, py: Python<'_>) -> PyResult<PyObject> {
        let result = PyList::empty_bound(py);

        for batch in &self.batches {
            let schema = batch.schema();
            for row_idx in 0..batch.num_rows() {
                let row_dict = PyDict::new_bound(py);

                for col_idx in 0..batch.num_columns() {
                    let field = schema.field(col_idx);
                    let column = batch.column(col_idx);
                    let value = array_value_to_python(py, column, row_idx)?;
                    row_dict.set_item(field.name(), value)?;
                }

                result.append(row_dict)?;
            }
        }

        Ok(result.into())
    }

    /// Convert to a dictionary of lists (column-oriented).
    ///
    /// Returns:
    ///     A dictionary mapping column names to lists of values.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let result = PyDict::new_bound(py);

        if self.batches.is_empty() {
            return Ok(result.into());
        }

        let schema = self.batches[0].schema();

        for col_idx in 0..schema.fields().len() {
            let field = schema.field(col_idx);
            let col_list = PyList::empty_bound(py);

            for batch in &self.batches {
                let column = batch.column(col_idx);
                for row_idx in 0..batch.num_rows() {
                    let value = array_value_to_python(py, column, row_idx)?;
                    col_list.append(value)?;
                }
            }

            result.set_item(field.name(), col_list)?;
        }

        Ok(result.into())
    }

    /// Convert to a PyArrow Table.
    ///
    /// Requires the pyarrow package to be installed.
    ///
    /// Returns:
    ///     A pyarrow.Table containing the query results.
    fn to_pyarrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        // Import pyarrow
        let pyarrow = py.import_bound("pyarrow")?;

        if self.batches.is_empty() {
            // Return empty table
            let empty_schema = pyarrow.call_method1("schema", (PyList::empty_bound(py),))?;
            return Ok(pyarrow
                .call_method1("table", (PyDict::new_bound(py), empty_schema))?
                .into());
        }

        // Convert each RecordBatch to a PyArrow RecordBatch
        let pa_batches = PyList::empty_bound(py);

        for batch in &self.batches {
            let pa_batch = record_batch_to_pyarrow(py, &pyarrow, batch)?;
            pa_batches.append(pa_batch)?;
        }

        // Concatenate batches into a Table
        let pa_table_class = pyarrow.getattr("Table")?;
        let table = pa_table_class.call_method1("from_batches", (pa_batches,))?;

        Ok(table.into())
    }

    /// Convert to a Polars DataFrame.
    ///
    /// Requires the polars package to be installed.
    ///
    /// Returns:
    ///     A polars.DataFrame containing the query results.
    fn to_polars(&self, py: Python<'_>) -> PyResult<PyObject> {
        // First convert to PyArrow, then to Polars
        let pa_table = self.to_pyarrow(py)?;

        // Import polars
        let polars = py.import_bound("polars")?;

        // Convert PyArrow Table to Polars DataFrame
        let df = polars.call_method1("from_arrow", (pa_table,))?;

        Ok(df.into())
    }

    /// Convert to a Pandas DataFrame.
    ///
    /// Requires the pandas package to be installed.
    ///
    /// Returns:
    ///     A pandas.DataFrame containing the query results.
    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        // First convert to PyArrow, then to Pandas
        let pa_table = self.to_pyarrow(py)?;

        // Call to_pandas on the PyArrow table
        let df = pa_table.call_method0(py, "to_pandas")?;

        Ok(df)
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryResult(rows={}, columns={})",
            self.num_rows(),
            self.num_columns()
        )
    }

    fn __len__(&self) -> usize {
        self.num_rows()
    }
}

/// Python wrapper for prepared statements.
#[pyclass(name = "PreparedStatement")]
pub struct PyPreparedStatement {
    inner: RustPreparedStatement,
}

#[pymethods]
impl PyPreparedStatement {
    /// Execute the prepared statement with the given parameters.
    ///
    /// Args:
    ///     *args: Positional parameters to substitute for $1, $2, etc.
    ///
    /// Returns:
    ///     A QueryResult containing the query results.
    #[pyo3(signature = (*args))]
    fn execute(&self, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<PyQueryResult> {
        let params = python_args_to_scalar_values(py, args)?;
        let batches = py_result(self.inner.execute(&params))?;
        Ok(PyQueryResult { batches })
    }

    /// Get information about the expected parameters.
    ///
    /// Returns:
    ///     A list of dictionaries with parameter information.
    fn parameters(&self) -> Vec<HashMap<String, String>> {
        self.inner
            .parameters()
            .iter()
            .map(|p| {
                let mut info = HashMap::new();
                info.insert("index".to_string(), p.index.to_string());
                info.insert("data_type".to_string(), format!("{:?}", p.data_type));
                info
            })
            .collect()
    }

    /// Get the SQL string for this prepared statement.
    fn sql(&self) -> &str {
        self.inner.sql()
    }

    fn __repr__(&self) -> String {
        format!(
            "PreparedStatement(sql='{}', params={})",
            self.inner.sql(),
            self.inner.parameters().len()
        )
    }
}

/// Python wrapper for schema information.
#[pyclass(name = "Schema")]
pub struct PySchema {
    inner: crate::Schema,
}

#[pymethods]
impl PySchema {
    /// Get the field names.
    fn names(&self) -> Vec<String> {
        self.inner
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    /// Get the field types as strings.
    fn types(&self) -> Vec<String> {
        self.inner
            .fields()
            .iter()
            .map(|f| format!("{:?}", f.data_type()))
            .collect()
    }

    /// Get the number of fields.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        let fields: Vec<String> = self
            .inner
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        format!("Schema({})", fields.join(", "))
    }
}

/// Convert a single array value to a Python object.
fn array_value_to_python(
    py: Python<'_>,
    array: &arrow::array::ArrayRef,
    row: usize,
) -> PyResult<PyObject> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row) {
        return Ok(py.None());
    }

    match array.data_type() {
        DataType::Null => Ok(py.None()),
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(arr.value(row).to_object(py))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            // Convert to ISO date string
            let days = arr.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            Ok(date.to_string().to_object(py))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            // Convert milliseconds to datetime
            let ms = arr.value(row);
            let secs = ms / 1000;
            let nsecs = ((ms % 1000) * 1_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                Ok(dt.to_rfc3339().to_object(py))
            } else {
                Ok(py.None())
            }
        }
        _ => {
            // Fallback: convert to string representation
            let formatted = arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| "?".to_string());
            Ok(formatted.to_object(py))
        }
    }
}

/// Convert Python arguments to ScalarValue vector.
fn python_args_to_scalar_values(
    _py: Python<'_>,
    args: &Bound<'_, PyTuple>,
) -> PyResult<Vec<ScalarValue>> {
    let mut result = Vec::with_capacity(args.len());

    for item in args.iter() {
        let scalar = python_to_scalar_value(&item)?;
        result.push(scalar);
    }

    Ok(result)
}

/// Convert a Python object to a ScalarValue.
fn python_to_scalar_value(obj: &Bound<'_, PyAny>) -> PyResult<ScalarValue> {
    if obj.is_none() {
        return Ok(ScalarValue::Null);
    }

    // Check for boolean first (before int, since bool is a subclass of int in Python)
    if let Ok(val) = obj.extract::<bool>() {
        return Ok(ScalarValue::Boolean(Some(val)));
    }

    // Check for integer
    if let Ok(val) = obj.extract::<i64>() {
        return Ok(ScalarValue::Int64(Some(val)));
    }

    // Check for float
    if let Ok(val) = obj.extract::<f64>() {
        return Ok(ScalarValue::Float64(Some(val)));
    }

    // Check for string
    if let Ok(val) = obj.extract::<String>() {
        return Ok(ScalarValue::Utf8(Some(val)));
    }

    // Check for bytes
    if let Ok(val) = obj.extract::<Vec<u8>>() {
        return Ok(ScalarValue::Binary(Some(val)));
    }

    Err(PyTypeError::new_err(format!(
        "Cannot convert Python object of type '{}' to ScalarValue",
        obj.get_type().name()?
    )))
}

/// Convert a Python dictionary to RecordBatches.
fn dict_to_record_batches(_py: Python<'_>, data: &Bound<'_, PyDict>) -> PyResult<Vec<RecordBatch>> {
    use arrow::array::*;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

    if data.is_empty() {
        return Err(PyValueError::new_err(
            "Cannot create table from empty dictionary",
        ));
    }

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (key, value) in data.iter() {
        let col_name: String = key.extract()?;
        let col_list = value
            .downcast::<PyList>()
            .map_err(|_| PyTypeError::new_err("Dictionary values must be lists"))?;

        if col_list.is_empty() {
            return Err(PyValueError::new_err(format!(
                "Column '{}' is empty",
                col_name
            )));
        }

        // Infer type from first non-null value
        let (dtype, array) = infer_and_build_array(&col_name, col_list)?;
        fields.push(Field::new(&col_name, dtype, true));
        arrays.push(array);
    }

    // Verify all columns have the same length
    let num_rows = arrays.first().map(|a| a.len()).unwrap_or(0);
    for (field, array) in fields.iter().zip(arrays.iter()) {
        if array.len() != num_rows {
            return Err(PyValueError::new_err(format!(
                "Column '{}' has {} rows, expected {}",
                field.name(),
                array.len(),
                num_rows
            )));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch =
        RecordBatch::try_new(schema, arrays).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(vec![batch])
}

/// Infer the Arrow data type and build an array from a Python list.
fn infer_and_build_array(
    col_name: &str,
    list: &Bound<'_, PyList>,
) -> PyResult<(arrow::datatypes::DataType, arrow::array::ArrayRef)> {
    use arrow::array::*;
    use arrow::datatypes::DataType as ArrowDataType;

    // Find first non-null value to infer type
    let mut inferred_type: Option<ArrowDataType> = None;

    for item in list.iter() {
        if !item.is_none() {
            if item.extract::<bool>().is_ok() {
                inferred_type = Some(ArrowDataType::Boolean);
            } else if item.extract::<i64>().is_ok() {
                inferred_type = Some(ArrowDataType::Int64);
            } else if item.extract::<f64>().is_ok() {
                inferred_type = Some(ArrowDataType::Float64);
            } else if item.extract::<String>().is_ok() {
                inferred_type = Some(ArrowDataType::Utf8);
            } else {
                return Err(PyTypeError::new_err(format!(
                    "Unsupported type in column '{}': {}",
                    col_name,
                    item.get_type().name()?
                )));
            }
            break;
        }
    }

    let dtype = inferred_type.unwrap_or(ArrowDataType::Null);

    // Build the array
    let array: ArrayRef = match dtype {
        ArrowDataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    builder.append_null();
                } else {
                    builder.append_value(item.extract::<bool>()?);
                }
            }
            Arc::new(builder.finish())
        }
        ArrowDataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    builder.append_null();
                } else {
                    builder.append_value(item.extract::<i64>()?);
                }
            }
            Arc::new(builder.finish())
        }
        ArrowDataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    builder.append_null();
                } else {
                    builder.append_value(item.extract::<f64>()?);
                }
            }
            Arc::new(builder.finish())
        }
        ArrowDataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(list.len(), list.len() * 32);
            for item in list.iter() {
                if item.is_none() {
                    builder.append_null();
                } else {
                    builder.append_value(item.extract::<String>()?);
                }
            }
            Arc::new(builder.finish())
        }
        ArrowDataType::Null => Arc::new(NullArray::new(list.len())),
        _ => unreachable!(),
    };

    Ok((dtype, array))
}

/// Convert a RecordBatch to a PyArrow RecordBatch.
fn record_batch_to_pyarrow(
    py: Python<'_>,
    pyarrow: &Bound<'_, PyModule>,
    batch: &RecordBatch,
) -> PyResult<PyObject> {
    // Build schema
    let pa_fields = PyList::empty_bound(py);
    for field in batch.schema().fields() {
        let pa_type = arrow_type_to_pyarrow(pyarrow, field.data_type())?;
        let pa_field =
            pyarrow.call_method1("field", (field.name(), pa_type, field.is_nullable()))?;
        pa_fields.append(pa_field)?;
    }
    let pa_schema = pyarrow.call_method1("schema", (pa_fields,))?;

    // Build arrays
    let pa_arrays = PyList::empty_bound(py);
    for col_idx in 0..batch.num_columns() {
        let column = batch.column(col_idx);
        let pa_array = arrow_array_to_pyarrow(py, pyarrow, column)?;
        pa_arrays.append(pa_array)?;
    }

    // Create RecordBatch
    let pa_record_batch_class = pyarrow.getattr("RecordBatch")?;
    let pa_batch = pa_record_batch_class.call_method1("from_arrays", (pa_arrays, pa_schema))?;

    Ok(pa_batch.into())
}

/// Convert an Arrow DataType to a PyArrow type.
fn arrow_type_to_pyarrow(
    pyarrow: &Bound<'_, PyModule>,
    dtype: &arrow::datatypes::DataType,
) -> PyResult<PyObject> {
    use arrow::datatypes::DataType;

    let type_fn = match dtype {
        DataType::Null => pyarrow.call_method0("null")?,
        DataType::Boolean => pyarrow.call_method0("bool_")?,
        DataType::Int8 => pyarrow.call_method0("int8")?,
        DataType::Int16 => pyarrow.call_method0("int16")?,
        DataType::Int32 => pyarrow.call_method0("int32")?,
        DataType::Int64 => pyarrow.call_method0("int64")?,
        DataType::UInt8 => pyarrow.call_method0("uint8")?,
        DataType::UInt16 => pyarrow.call_method0("uint16")?,
        DataType::UInt32 => pyarrow.call_method0("uint32")?,
        DataType::UInt64 => pyarrow.call_method0("uint64")?,
        DataType::Float32 => pyarrow.call_method0("float32")?,
        DataType::Float64 => pyarrow.call_method0("float64")?,
        DataType::Utf8 => pyarrow.call_method0("utf8")?,
        DataType::LargeUtf8 => pyarrow.call_method0("large_utf8")?,
        DataType::Binary => pyarrow.call_method0("binary")?,
        DataType::LargeBinary => pyarrow.call_method0("large_binary")?,
        DataType::Date32 => pyarrow.call_method0("date32")?,
        DataType::Date64 => pyarrow.call_method0("date64")?,
        _ => {
            // Fallback to string for unsupported types
            pyarrow.call_method0("utf8")?
        }
    };

    Ok(type_fn.into())
}

/// Convert an Arrow Array to a PyArrow Array.
fn arrow_array_to_pyarrow(
    _py: Python<'_>,
    pyarrow: &Bound<'_, PyModule>,
    array: &arrow::array::ArrayRef,
) -> PyResult<PyObject> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    let pa_array = pyarrow.getattr("array")?;

    match array.data_type() {
        DataType::Null => {
            let nulls: Vec<Option<i64>> = vec![None; array.len()];
            Ok(pa_array.call1((nulls,))?.into())
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let values: Vec<Option<bool>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            let values: Vec<Option<i8>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            let values: Vec<Option<i16>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            let values: Vec<Option<i32>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let values: Vec<Option<i64>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            let values: Vec<Option<u8>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            let values: Vec<Option<u16>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            let values: Vec<Option<u32>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            let values: Vec<Option<u64>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let values: Vec<Option<f32>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let values: Vec<Option<f64>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let values: Vec<Option<&str>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let values: Vec<Option<&str>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let values: Vec<Option<&[u8]>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let values: Vec<Option<&[u8]>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
        _ => {
            // Fallback: convert to string array
            let values: Vec<Option<String>> = (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        Some(
                            arrow::util::display::array_value_to_string(array, i)
                                .unwrap_or_else(|_| "?".to_string()),
                        )
                    }
                })
                .collect();
            Ok(pa_array.call1((values,))?.into())
        }
    }
}

/// Create a new in-memory connection.
///
/// This is a convenience function equivalent to `Connection()`.
///
/// Args:
///     batch_size: The batch size for vectorized execution (default: 8192).
///     memory_limit: Optional memory limit in bytes.
///
/// Returns:
///     A new Connection object.
#[pyfunction]
#[pyo3(signature = (*, batch_size=8192, memory_limit=None))]
fn connect(batch_size: usize, memory_limit: Option<usize>) -> PyResult<PyConnection> {
    PyConnection::new(batch_size, memory_limit)
}

/// Read a CSV file and return a QueryResult.
///
/// Args:
///     path: The path to the CSV file.
///     delimiter: Optional delimiter character (default: ',').
///     has_header: Whether the file has a header row (default: True).
///
/// Returns:
///     A QueryResult containing the file contents.
#[pyfunction]
#[pyo3(signature = (path, *, delimiter=None, has_header=true))]
fn read_csv(path: &str, delimiter: Option<char>, has_header: bool) -> PyResult<PyQueryResult> {
    let mut options = crate::storage::CsvOptions::default();
    if let Some(d) = delimiter {
        options.delimiter = d as u8;
    }
    options.has_header = has_header;

    let table = py_result(crate::storage::CsvTable::open_with_options(path, options))?;
    let batches = py_result(table.scan(None, &[], None))?;
    Ok(PyQueryResult { batches })
}

/// Read a Parquet file and return a QueryResult.
///
/// Args:
///     path: The path to the Parquet file.
///
/// Returns:
///     A QueryResult containing the file contents.
#[pyfunction]
fn read_parquet(path: &str) -> PyResult<PyQueryResult> {
    let table = py_result(crate::storage::ParquetTable::open(path))?;
    let batches = py_result(table.scan(None, &[], None))?;
    Ok(PyQueryResult { batches })
}

/// Read a Delta Lake table and return a QueryResult.
///
/// Args:
///     path: The path to the Delta table directory.
///     version: Optional version number for time travel (reads latest if not specified).
///
/// Returns:
///     A QueryResult containing the table contents.
#[pyfunction]
#[pyo3(signature = (path, *, version=None))]
fn read_delta(path: &str, version: Option<i64>) -> PyResult<PyQueryResult> {
    let table = match version {
        Some(v) => py_result(crate::storage::DeltaTable::open_at_version(path, v))?,
        None => py_result(crate::storage::DeltaTable::open(path))?,
    };
    let batches = py_result(table.scan(None, &[], None))?;
    Ok(PyQueryResult { batches })
}

/// The PyBlaze Python module.
///
/// A high-performance embedded OLAP query engine with native
/// Apache Arrow and Parquet support.
#[pymodule]
fn pyblaze(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyConnection>()?;
    m.add_class::<PyQueryResult>()?;
    m.add_class::<PyPreparedStatement>()?;
    m.add_class::<PySchema>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_delta, m)?)?;

    // Add version info
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_value_conversion() {
        // Test that ScalarValue types are properly defined
        let int_val = ScalarValue::Int64(Some(42));
        assert!(matches!(int_val, ScalarValue::Int64(Some(42))));

        let str_val = ScalarValue::Utf8(Some("hello".to_string()));
        assert!(matches!(str_val, ScalarValue::Utf8(Some(_))));

        let null_val = ScalarValue::Null;
        assert!(matches!(null_val, ScalarValue::Null));
    }
}
