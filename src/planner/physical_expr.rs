//! Physical expressions for query execution.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Int64Array, RecordBatch,
};
use arrow::compute::kernels::boolean;
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType as ArrowDataType;

use crate::error::{BlazeError, Result};
use crate::types::ScalarValue;

/// A physical expression that can be evaluated against a RecordBatch.
pub trait PhysicalExpr: Debug + Send + Sync {
    /// Return self as Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Get the data type of this expression.
    fn data_type(&self) -> ArrowDataType;

    /// Evaluate this expression against a batch.
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Get the name of this expression.
    fn name(&self) -> &str;
}

/// Column reference expression.
#[derive(Debug)]
pub struct ColumnExpr {
    name: String,
    index: usize,
}

impl ColumnExpr {
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        Self {
            name: name.into(),
            index,
        }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        // Data type will be determined at evaluation time
        ArrowDataType::Null
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        Ok(batch.column(self.index).clone())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Literal expression.
#[derive(Debug)]
pub struct LiteralExpr {
    value: ScalarValue,
}

impl LiteralExpr {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl PhysicalExpr for LiteralExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        self.value.data_type().to_arrow()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.value.to_array_of_size(batch.num_rows())
    }

    fn name(&self) -> &str {
        "literal"
    }
}

/// Binary expression.
#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: String,
    right: Arc<dyn PhysicalExpr>,
    name: String,
}

impl BinaryExpr {
    pub fn new(left: Arc<dyn PhysicalExpr>, op: &str, right: Arc<dyn PhysicalExpr>) -> Self {
        let name = format!("{} {} {}", left.name(), op, right.name());
        Self {
            left,
            op: op.to_string(),
            right,
            name,
        }
    }
}

impl PhysicalExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        match self.op.as_str() {
            "eq" | "neq" | "lt" | "lte" | "gt" | "gte" | "and" | "or" => {
                ArrowDataType::Boolean
            }
            _ => self.left.data_type(),
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let left = self.left.evaluate(batch)?;
        let right = self.right.evaluate(batch)?;

        match self.op.as_str() {
            "eq" => {
                let result = cmp::eq(&left, &right)?;
                Ok(Arc::new(result))
            }
            "neq" => {
                let result = cmp::neq(&left, &right)?;
                Ok(Arc::new(result))
            }
            "lt" => {
                let result = cmp::lt(&left, &right)?;
                Ok(Arc::new(result))
            }
            "lte" => {
                let result = cmp::lt_eq(&left, &right)?;
                Ok(Arc::new(result))
            }
            "gt" => {
                let result = cmp::gt(&left, &right)?;
                Ok(Arc::new(result))
            }
            "gte" => {
                let result = cmp::gt_eq(&left, &right)?;
                Ok(Arc::new(result))
            }
            "and" => {
                let left_bool = left.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected boolean array"))?;
                let right_bool = right.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected boolean array"))?;
                let result = boolean::and(left_bool, right_bool)?;
                Ok(Arc::new(result))
            }
            "or" => {
                let left_bool = left.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected boolean array"))?;
                let right_bool = right.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected boolean array"))?;
                let result = boolean::or(left_bool, right_bool)?;
                Ok(Arc::new(result))
            }
            "plus" | "minus" | "multiply" | "divide" => {
                self.evaluate_arithmetic(&left, &right)
            }
            _ => Err(BlazeError::not_implemented(format!("Operator: {}", self.op))),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl BinaryExpr {
    fn evaluate_arithmetic(&self, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        use arrow::compute::kernels::numeric;

        match (left.data_type(), right.data_type()) {
            (ArrowDataType::Int64, ArrowDataType::Int64) => {
                let left = left.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64 array"))?;
                let right = right.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64 array"))?;

                let result = match self.op.as_str() {
                    "plus" => numeric::add(left, right)?,
                    "minus" => numeric::sub(left, right)?,
                    "multiply" => numeric::mul(left, right)?,
                    "divide" => numeric::div(left, right)?,
                    _ => return Err(BlazeError::internal("Invalid arithmetic op")),
                };
                Ok(Arc::new(result))
            }
            (ArrowDataType::Float64, ArrowDataType::Float64) => {
                let left = left.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Float64 array"))?;
                let right = right.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Float64 array"))?;

                let result = match self.op.as_str() {
                    "plus" => numeric::add(left, right)?,
                    "minus" => numeric::sub(left, right)?,
                    "multiply" => numeric::mul(left, right)?,
                    "divide" => numeric::div(left, right)?,
                    _ => return Err(BlazeError::internal("Invalid arithmetic op")),
                };
                Ok(Arc::new(result))
            }
            _ => Err(BlazeError::type_error(format!(
                "Unsupported arithmetic types: {:?} and {:?}",
                left.data_type(),
                right.data_type()
            ))),
        }
    }
}

/// NOT expression.
#[derive(Debug)]
pub struct NotExpr {
    expr: Arc<dyn PhysicalExpr>,
}

impl NotExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for NotExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let result = self.expr.evaluate(batch)?;
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| BlazeError::type_error("Expected boolean array"))?;
        let negated = boolean::not(bool_arr)?;
        Ok(Arc::new(negated))
    }

    fn name(&self) -> &str {
        "NOT"
    }
}

/// Bitwise NOT expression (~).
#[derive(Debug)]
pub struct BitwiseNotExpr {
    expr: Arc<dyn PhysicalExpr>,
}

impl BitwiseNotExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for BitwiseNotExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        self.expr.data_type()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let result = self.expr.evaluate(batch)?;

        match result.data_type() {
            ArrowDataType::Int8 => {
                let arr = result.as_any().downcast_ref::<arrow::array::Int8Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int8 array"))?;
                let negated: arrow::array::Int8Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            ArrowDataType::Int16 => {
                let arr = result.as_any().downcast_ref::<arrow::array::Int16Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int16 array"))?;
                let negated: arrow::array::Int16Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            ArrowDataType::Int32 => {
                let arr = result.as_any().downcast_ref::<arrow::array::Int32Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int32 array"))?;
                let negated: arrow::array::Int32Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            ArrowDataType::Int64 => {
                let arr = result.as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64 array"))?;
                let negated: arrow::array::Int64Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            ArrowDataType::UInt8 => {
                let arr = result.as_any().downcast_ref::<arrow::array::UInt8Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected UInt8 array"))?;
                let negated: arrow::array::UInt8Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            ArrowDataType::UInt16 => {
                let arr = result.as_any().downcast_ref::<arrow::array::UInt16Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected UInt16 array"))?;
                let negated: arrow::array::UInt16Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            ArrowDataType::UInt32 => {
                let arr = result.as_any().downcast_ref::<arrow::array::UInt32Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected UInt32 array"))?;
                let negated: arrow::array::UInt32Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            ArrowDataType::UInt64 => {
                let arr = result.as_any().downcast_ref::<arrow::array::UInt64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected UInt64 array"))?;
                let negated: arrow::array::UInt64Array = arr.iter()
                    .map(|opt| opt.map(|v| !v))
                    .collect();
                Ok(Arc::new(negated))
            }
            dt => Err(BlazeError::type_error(format!(
                "Bitwise NOT not supported for type {:?}. Supported: Int8-64, UInt8-64",
                dt
            )))
        }
    }

    fn name(&self) -> &str {
        "BITWISE_NOT"
    }
}

/// IS NULL expression.
#[derive(Debug)]
pub struct IsNullExpr {
    expr: Arc<dyn PhysicalExpr>,
}

impl IsNullExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for IsNullExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let result = self.expr.evaluate(batch)?;
        let null_check = arrow::compute::is_null(&result)?;
        Ok(Arc::new(null_check))
    }

    fn name(&self) -> &str {
        "IS NULL"
    }
}

/// IS NOT NULL expression.
#[derive(Debug)]
pub struct IsNotNullExpr {
    expr: Arc<dyn PhysicalExpr>,
}

impl IsNotNullExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for IsNotNullExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let result = self.expr.evaluate(batch)?;
        let not_null_check = arrow::compute::is_not_null(&result)?;
        Ok(Arc::new(not_null_check))
    }

    fn name(&self) -> &str {
        "IS NOT NULL"
    }
}

/// CAST expression.
#[derive(Debug)]
pub struct CastExpr {
    expr: Arc<dyn PhysicalExpr>,
    target_type: ArrowDataType,
}

impl CastExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, target_type: ArrowDataType) -> Self {
        Self { expr, target_type }
    }
}

impl PhysicalExpr for CastExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        self.target_type.clone()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let value = self.expr.evaluate(batch)?;
        let cast_result = arrow::compute::cast(&value, &self.target_type)?;
        Ok(cast_result)
    }

    fn name(&self) -> &str {
        "CAST"
    }
}

/// CASE WHEN expression.
#[derive(Debug)]
pub struct CaseExpr {
    /// Optional operand for simple CASE (CASE expr WHEN ...)
    operand: Option<Arc<dyn PhysicalExpr>>,
    /// List of (when_condition, then_result) pairs
    when_then: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    /// Optional else result
    else_result: Option<Arc<dyn PhysicalExpr>>,
}

impl CaseExpr {
    pub fn new(
        operand: Option<Arc<dyn PhysicalExpr>>,
        when_then: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
        else_result: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self { operand, when_then, else_result }
    }
}

impl PhysicalExpr for CaseExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        // Return type of first THEN clause (or ELSE)
        if let Some((_, then_expr)) = self.when_then.first() {
            then_expr.data_type()
        } else if let Some(else_expr) = &self.else_result {
            else_expr.data_type()
        } else {
            ArrowDataType::Null
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        use arrow::array::new_null_array;
        use arrow::compute::kernels::zip::zip;

        let num_rows = batch.num_rows();

        // Start with NULL or ELSE result
        let mut result = if let Some(else_expr) = &self.else_result {
            else_expr.evaluate(batch)?
        } else {
            new_null_array(&self.data_type(), num_rows)
        };

        // Evaluate WHEN/THEN clauses in reverse order (last match wins)
        for (when_expr, then_expr) in self.when_then.iter().rev() {
            let condition = if let Some(operand) = &self.operand {
                // Simple CASE: compare operand = when_expr
                let op_val = operand.evaluate(batch)?;
                let when_val = when_expr.evaluate(batch)?;
                Arc::new(cmp::eq(&op_val, &when_val)?) as ArrayRef
            } else {
                // Searched CASE: when_expr is already a boolean condition
                when_expr.evaluate(batch)?
            };

            let cond_bool = condition.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| BlazeError::type_error("CASE condition must be boolean"))?;

            let then_val = then_expr.evaluate(batch)?;

            // Use zip to select between then_val and current result
            result = zip(cond_bool, &then_val, &result)?;
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "CASE"
    }
}

/// BETWEEN expression (expr BETWEEN low AND high).
#[derive(Debug)]
pub struct BetweenExpr {
    expr: Arc<dyn PhysicalExpr>,
    low: Arc<dyn PhysicalExpr>,
    high: Arc<dyn PhysicalExpr>,
    negated: bool,
}

impl BetweenExpr {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        low: Arc<dyn PhysicalExpr>,
        high: Arc<dyn PhysicalExpr>,
        negated: bool,
    ) -> Self {
        Self { expr, low, high, negated }
    }
}

impl PhysicalExpr for BetweenExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let value = self.expr.evaluate(batch)?;
        let low = self.low.evaluate(batch)?;
        let high = self.high.evaluate(batch)?;

        // value >= low AND value <= high
        let gte_low = cmp::gt_eq(&value, &low)?;
        let lte_high = cmp::lt_eq(&value, &high)?;
        let result = boolean::and(&gte_low, &lte_high)?;

        if self.negated {
            Ok(Arc::new(boolean::not(&result)?))
        } else {
            Ok(Arc::new(result))
        }
    }

    fn name(&self) -> &str {
        if self.negated { "NOT BETWEEN" } else { "BETWEEN" }
    }
}

/// LIKE expression for pattern matching.
#[derive(Debug)]
pub struct LikeExpr {
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
    negated: bool,
    case_insensitive: bool,
}

impl LikeExpr {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        pattern: Arc<dyn PhysicalExpr>,
        negated: bool,
        case_insensitive: bool,
    ) -> Self {
        Self { expr, pattern, negated, case_insensitive }
    }
}

impl PhysicalExpr for LikeExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        use arrow::array::StringArray;
        use arrow::compute::kernels::comparison::{like, ilike, nlike, nilike};

        let value = self.expr.evaluate(batch)?;
        let pattern = self.pattern.evaluate(batch)?;

        let value_str = value.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| BlazeError::type_error("LIKE requires string operand"))?;
        let pattern_str = pattern.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| BlazeError::type_error("LIKE requires string pattern"))?;

        let result = match (self.negated, self.case_insensitive) {
            (false, false) => like(value_str, pattern_str)?,
            (false, true) => ilike(value_str, pattern_str)?,
            (true, false) => nlike(value_str, pattern_str)?,
            (true, true) => nilike(value_str, pattern_str)?,
        };

        Ok(Arc::new(result))
    }

    fn name(&self) -> &str {
        match (self.negated, self.case_insensitive) {
            (false, false) => "LIKE",
            (false, true) => "ILIKE",
            (true, false) => "NOT LIKE",
            (true, true) => "NOT ILIKE",
        }
    }
}

/// IN list expression.
#[derive(Debug)]
pub struct InListExpr {
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
}

impl InListExpr {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
    ) -> Self {
        Self { expr, list, negated }
    }
}

impl PhysicalExpr for InListExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let value = self.expr.evaluate(batch)?;
        let num_rows = batch.num_rows();

        // Start with all false
        let mut result = BooleanArray::from(vec![false; num_rows]);

        // OR together comparisons with each list element
        for list_expr in &self.list {
            let list_val = list_expr.evaluate(batch)?;
            let eq_result = cmp::eq(&value, &list_val)?;
            result = boolean::or(&result, &eq_result)?;
        }

        if self.negated {
            Ok(Arc::new(boolean::not(&result)?))
        } else {
            Ok(Arc::new(result))
        }
    }

    fn name(&self) -> &str {
        if self.negated { "NOT IN" } else { "IN" }
    }
}

/// Scalar function expression.
#[derive(Debug)]
pub struct ScalarFunctionExpr {
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
}

impl ScalarFunctionExpr {
    pub fn new(name: impl Into<String>, args: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self { name: name.into(), args }
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        match self.name.to_uppercase().as_str() {
            // String functions
            "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "CONCAT"
            | "REPLACE" | "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT"
            | "LPAD" | "RPAD" | "REVERSE" | "SPLIT_PART" | "REGEXP_REPLACE" => ArrowDataType::Utf8,
            "LENGTH" | "CHAR_LENGTH" => ArrowDataType::Int64,
            "REGEXP_MATCH" => ArrowDataType::Boolean,
            "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" => {
                if !self.args.is_empty() {
                    self.args[0].data_type()
                } else {
                    ArrowDataType::Float64
                }
            }
            "COALESCE" | "NULLIF" | "IFNULL" | "NVL" | "GREATEST" | "LEAST" => {
                if !self.args.is_empty() {
                    self.args[0].data_type()
                } else {
                    ArrowDataType::Null
                }
            }
            // Date/Time functions
            "CURRENT_DATE" | "TO_DATE" => ArrowDataType::Date32,
            "CURRENT_TIMESTAMP" | "NOW" | "DATE_TRUNC" | "TO_TIMESTAMP"
            | "DATE_ADD" | "DATEADD" | "DATE_SUB" | "DATESUB" => {
                if !self.args.is_empty() {
                    self.args[0].data_type()
                } else {
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                }
            }
            "EXTRACT" | "DATE_PART" | "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND"
            | "DATE_DIFF" | "DATEDIFF" => ArrowDataType::Int64,
            // JSON functions
            "JSON_EXTRACT" | "JSON_VALUE" | "JSON_OBJECT" | "JSON_ARRAY"
            | "JSON_TYPE" | "JSON_KEYS" => ArrowDataType::Utf8,
            "JSON_EXTRACT_INT" | "JSON_LENGTH" => ArrowDataType::Int64,
            "JSON_EXTRACT_FLOAT" => ArrowDataType::Float64,
            "JSON_EXTRACT_BOOL" | "JSON_VALID" | "JSON_CONTAINS_KEY" => ArrowDataType::Boolean,
            _ => ArrowDataType::Utf8,
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        use arrow::array::{StringArray, StringBuilder};
        use arrow::compute::kernels::length::length;

        match self.name.to_uppercase().as_str() {
            "UPPER" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("UPPER requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let str_arr = arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("UPPER requires string argument"))?;

                let result: StringArray = str_arr.iter()
                    .map(|opt| opt.map(|s| s.to_uppercase()))
                    .collect();
                Ok(Arc::new(result))
            }

            "LOWER" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("LOWER requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let str_arr = arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("LOWER requires string argument"))?;

                let result: StringArray = str_arr.iter()
                    .map(|opt| opt.map(|s| s.to_lowercase()))
                    .collect();
                Ok(Arc::new(result))
            }

            "LENGTH" | "CHAR_LENGTH" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("LENGTH requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let len_arr = length(&arg)?;
                // Convert to Int64
                let cast_arr = arrow::compute::cast(&len_arr, &ArrowDataType::Int64)?;
                Ok(cast_arr)
            }

            "CONCAT" => {
                if self.args.is_empty() {
                    return Ok(Arc::new(StringArray::from(vec![""; batch.num_rows()])));
                }

                // Evaluate all arguments
                let evaluated: Vec<ArrayRef> = self.args.iter()
                    .map(|a| a.evaluate(batch))
                    .collect::<Result<Vec<_>>>()?;

                let string_arrays: Vec<&StringArray> = evaluated.iter()
                    .map(|a| a.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| BlazeError::type_error("CONCAT requires string arguments")))
                    .collect::<Result<Vec<_>>>()?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    let mut concat = String::new();
                    let mut has_null = false;
                    for arr in &string_arrays {
                        if arr.is_null(i) {
                            has_null = true;
                            break;
                        }
                        concat.push_str(arr.value(i));
                    }
                    if has_null {
                        builder.append_null();
                    } else {
                        builder.append_value(&concat);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "COALESCE" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("COALESCE requires at least 1 argument"));
                }

                let first = self.args[0].evaluate(batch)?;
                let mut result = first;

                for arg_expr in self.args.iter().skip(1) {
                    let arg = arg_expr.evaluate(batch)?;
                    // Use zip to select non-null values
                    let is_null = arrow::compute::is_null(&result)?;
                    result = arrow::compute::kernels::zip::zip(&is_null, &arg, &result)?;
                }

                Ok(result)
            }

            "ABS" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("ABS requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;

                match arg.data_type() {
                    ArrowDataType::Int64 => {
                        let arr = arg.as_any().downcast_ref::<Int64Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Int64"))?;
                        let result: Int64Array = arr.iter()
                            .map(|opt| opt.map(|v| v.abs()))
                            .collect();
                        Ok(Arc::new(result))
                    }
                    ArrowDataType::Float64 => {
                        let arr = arg.as_any().downcast_ref::<Float64Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Float64"))?;
                        let result: Float64Array = arr.iter()
                            .map(|opt| opt.map(|v| v.abs()))
                            .collect();
                        Ok(Arc::new(result))
                    }
                    _ => Err(BlazeError::type_error("ABS requires numeric argument")),
                }
            }

            "TRIM" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("TRIM requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let str_arr = arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("TRIM requires string argument"))?;

                let result: StringArray = str_arr.iter()
                    .map(|opt| opt.map(|s| s.trim().to_string()))
                    .collect();
                Ok(Arc::new(result))
            }

            "LTRIM" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("LTRIM requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let str_arr = arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("LTRIM requires string argument"))?;

                let result: StringArray = str_arr.iter()
                    .map(|opt| opt.map(|s| s.trim_start().to_string()))
                    .collect();
                Ok(Arc::new(result))
            }

            "RTRIM" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("RTRIM requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let str_arr = arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("RTRIM requires string argument"))?;

                let result: StringArray = str_arr.iter()
                    .map(|opt| opt.map(|s| s.trim_end().to_string()))
                    .collect();
                Ok(Arc::new(result))
            }

            "ROUND" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("ROUND requires at least 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let precision = if self.args.len() > 1 {
                    let prec_arg = self.args[1].evaluate(batch)?;
                    let prec_arr = prec_arg.as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| BlazeError::type_error("ROUND precision must be integer"))?;
                    if prec_arr.is_null(0) {
                        0i32
                    } else {
                        prec_arr.value(0) as i32
                    }
                } else {
                    0i32
                };

                match arg.data_type() {
                    ArrowDataType::Float64 => {
                        let arr = arg.as_any().downcast_ref::<Float64Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Float64"))?;
                        let multiplier = 10f64.powi(precision);
                        let result: Float64Array = arr.iter()
                            .map(|opt| opt.map(|v| (v * multiplier).round() / multiplier))
                            .collect();
                        Ok(Arc::new(result))
                    }
                    ArrowDataType::Int64 => {
                        // Integer rounding with negative precision
                        if precision < 0 {
                            let arr = arg.as_any().downcast_ref::<Int64Array>()
                                .ok_or_else(|| BlazeError::type_error("Expected Int64"))?;
                            let divisor = 10i64.pow((-precision) as u32);
                            let result: Int64Array = arr.iter()
                                .map(|opt| opt.map(|v| ((v as f64 / divisor as f64).round() as i64) * divisor))
                                .collect();
                            Ok(Arc::new(result))
                        } else {
                            Ok(arg)
                        }
                    }
                    _ => Err(BlazeError::type_error("ROUND requires numeric argument")),
                }
            }

            "CEIL" | "CEILING" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("CEIL requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;

                match arg.data_type() {
                    ArrowDataType::Float64 => {
                        let arr = arg.as_any().downcast_ref::<Float64Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Float64"))?;
                        let result: Float64Array = arr.iter()
                            .map(|opt| opt.map(|v| v.ceil()))
                            .collect();
                        Ok(Arc::new(result))
                    }
                    ArrowDataType::Int64 => Ok(arg),
                    _ => Err(BlazeError::type_error("CEIL requires numeric argument")),
                }
            }

            "FLOOR" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("FLOOR requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;

                match arg.data_type() {
                    ArrowDataType::Float64 => {
                        let arr = arg.as_any().downcast_ref::<Float64Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Float64"))?;
                        let result: Float64Array = arr.iter()
                            .map(|opt| opt.map(|v| v.floor()))
                            .collect();
                        Ok(Arc::new(result))
                    }
                    ArrowDataType::Int64 => Ok(arg),
                    _ => Err(BlazeError::type_error("FLOOR requires numeric argument")),
                }
            }

            "NULLIF" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("NULLIF requires 2 arguments"));
                }
                let arg1 = self.args[0].evaluate(batch)?;
                let arg2 = self.args[1].evaluate(batch)?;

                // Compare the two arrays - if equal, return null; otherwise return arg1
                let eq_result = arrow::compute::kernels::cmp::eq(&arg1, &arg2)?;

                // For NULLIF: when equal (true), return null; otherwise return arg1
                // We need to create a null array for the null case
                let null_array = arrow::array::new_null_array(arg1.data_type(), batch.num_rows());
                let result = arrow::compute::kernels::zip::zip(&eq_result, &null_array, &arg1)?;
                Ok(result)
            }

            // Date/Time functions
            "CURRENT_DATE" => {
                use chrono::Datelike;
                let today = chrono::Local::now().date_naive();
                let days_since_epoch = today.num_days_from_ce() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().num_days_from_ce();
                let result: arrow::array::Date32Array = (0..batch.num_rows())
                    .map(|_| Some(days_since_epoch))
                    .collect();
                Ok(Arc::new(result))
            }

            "CURRENT_TIMESTAMP" | "NOW" => {
                let now = chrono::Local::now();
                let ts_micros = now.timestamp_micros();
                let result: arrow::array::TimestampMicrosecondArray = (0..batch.num_rows())
                    .map(|_| Some(ts_micros))
                    .collect();
                Ok(Arc::new(result))
            }

            "EXTRACT" | "DATE_PART" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("EXTRACT requires 2 arguments (part, date)"));
                }
                let part_arg = self.args[0].evaluate(batch)?;
                let date_arg = self.args[1].evaluate(batch)?;

                let part_arr = part_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("EXTRACT part must be a string"))?;
                let part = part_arr.value(0).to_uppercase();

                match date_arg.data_type() {
                    ArrowDataType::Date32 => {
                        use chrono::Datelike;
                        let date_arr = date_arg.as_any().downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Date32"))?;
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let result: Int64Array = date_arr.iter()
                            .map(|opt| {
                                opt.map(|days| {
                                    let date = epoch + chrono::Duration::days(days as i64);
                                    match part.as_str() {
                                        "YEAR" => date.year() as i64,
                                        "MONTH" => date.month() as i64,
                                        "DAY" => date.day() as i64,
                                        "DOW" | "DAYOFWEEK" => date.weekday().num_days_from_sunday() as i64,
                                        "DOY" | "DAYOFYEAR" => date.ordinal() as i64,
                                        "WEEK" => date.iso_week().week() as i64,
                                        "QUARTER" => ((date.month() - 1) / 3 + 1) as i64,
                                        _ => 0,
                                    }
                                })
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }
                    ArrowDataType::Timestamp(_, _) => {
                        use chrono::{Datelike, Timelike};
                        let ts_arr = date_arg.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                            .ok_or_else(|| BlazeError::type_error("Expected Timestamp"))?;
                        let result: Int64Array = ts_arr.iter()
                            .map(|opt| {
                                opt.map(|micros| {
                                    let dt = chrono::DateTime::from_timestamp_micros(micros)
                                        .map(|dt| dt.naive_utc())
                                        .unwrap_or_else(|| chrono::NaiveDateTime::default());
                                    match part.as_str() {
                                        "YEAR" => dt.year() as i64,
                                        "MONTH" => dt.month() as i64,
                                        "DAY" => dt.day() as i64,
                                        "HOUR" => dt.hour() as i64,
                                        "MINUTE" => dt.minute() as i64,
                                        "SECOND" => dt.second() as i64,
                                        "DOW" | "DAYOFWEEK" => dt.weekday().num_days_from_sunday() as i64,
                                        "DOY" | "DAYOFYEAR" => dt.ordinal() as i64,
                                        "WEEK" => dt.iso_week().week() as i64,
                                        "QUARTER" => ((dt.month() - 1) / 3 + 1) as i64,
                                        "EPOCH" => micros / 1_000_000,
                                        _ => 0,
                                    }
                                })
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }
                    _ => Err(BlazeError::type_error("EXTRACT requires date/timestamp argument")),
                }
            }

            "DATE_TRUNC" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("DATE_TRUNC requires 2 arguments (precision, timestamp)"));
                }
                let part_arg = self.args[0].evaluate(batch)?;
                let ts_arg = self.args[1].evaluate(batch)?;

                let part_arr = part_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("DATE_TRUNC precision must be a string"))?;
                let part = part_arr.value(0).to_uppercase();

                let ts_arr = ts_arg.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or_else(|| BlazeError::type_error("DATE_TRUNC requires timestamp argument"))?;

                use chrono::{Datelike, Timelike, NaiveDate, NaiveDateTime, NaiveTime};
                let result: arrow::array::TimestampMicrosecondArray = ts_arr.iter()
                    .map(|opt| {
                        opt.map(|micros| {
                            let dt = chrono::DateTime::from_timestamp_micros(micros)
                                .map(|dt| dt.naive_utc())
                                .unwrap_or_else(|| NaiveDateTime::default());
                            let truncated = match part.as_str() {
                                "YEAR" => NaiveDateTime::new(
                                    NaiveDate::from_ymd_opt(dt.year(), 1, 1).unwrap(),
                                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                                ),
                                "MONTH" => NaiveDateTime::new(
                                    NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).unwrap(),
                                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                                ),
                                "DAY" => NaiveDateTime::new(
                                    dt.date(),
                                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                                ),
                                "HOUR" => NaiveDateTime::new(
                                    dt.date(),
                                    NaiveTime::from_hms_opt(dt.hour(), 0, 0).unwrap(),
                                ),
                                "MINUTE" => NaiveDateTime::new(
                                    dt.date(),
                                    NaiveTime::from_hms_opt(dt.hour(), dt.minute(), 0).unwrap(),
                                ),
                                _ => dt,
                            };
                            truncated.and_utc().timestamp_micros()
                        })
                    })
                    .collect();
                Ok(Arc::new(result))
            }

            // JSON Functions
            "JSON_EXTRACT" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("JSON_EXTRACT requires 2 arguments"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let path_arg = self.args[1].evaluate(batch)?;

                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_EXTRACT requires string argument"))?;
                let path_arr = path_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_EXTRACT requires string path"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if json_arr.is_null(i) || path_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        match crate::json::json_extract(json_arr.value(i), path_arr.value(i)) {
                            Ok(Some(v)) => builder.append_value(&v),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "JSON_VALUE" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("JSON_VALUE requires 2 arguments"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let path_arg = self.args[1].evaluate(batch)?;

                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_VALUE requires string argument"))?;
                let path_arr = path_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_VALUE requires string path"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if json_arr.is_null(i) || path_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        match crate::json::json_value(json_arr.value(i), path_arr.value(i)) {
                            Ok(Some(v)) => builder.append_value(&v),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "JSON_EXTRACT_INT" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("JSON_EXTRACT_INT requires 2 arguments"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let path_arg = self.args[1].evaluate(batch)?;

                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_EXTRACT_INT requires string argument"))?;
                let path_arr = path_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_EXTRACT_INT requires string path"))?;

                let result: Int64Array = (0..batch.num_rows())
                    .map(|i| {
                        if json_arr.is_null(i) || path_arr.is_null(i) {
                            None
                        } else {
                            crate::json::json_extract_int(json_arr.value(i), path_arr.value(i))
                                .ok()
                                .flatten()
                        }
                    })
                    .collect();
                Ok(Arc::new(result))
            }

            "JSON_EXTRACT_FLOAT" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("JSON_EXTRACT_FLOAT requires 2 arguments"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let path_arg = self.args[1].evaluate(batch)?;

                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_EXTRACT_FLOAT requires string argument"))?;
                let path_arr = path_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_EXTRACT_FLOAT requires string path"))?;

                let result: Float64Array = (0..batch.num_rows())
                    .map(|i| {
                        if json_arr.is_null(i) || path_arr.is_null(i) {
                            None
                        } else {
                            crate::json::json_extract_float(json_arr.value(i), path_arr.value(i))
                                .ok()
                                .flatten()
                        }
                    })
                    .collect();
                Ok(Arc::new(result))
            }

            "JSON_VALID" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("JSON_VALID requires 1 argument"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_VALID requires string argument"))?;

                let result: BooleanArray = json_arr.iter()
                    .map(|opt| opt.map(crate::json::json_valid))
                    .collect();
                Ok(Arc::new(result))
            }

            "JSON_TYPE" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("JSON_TYPE requires 1 argument"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_TYPE requires string argument"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if json_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        match crate::json::json_type(json_arr.value(i)) {
                            Ok(Some(v)) => builder.append_value(&v),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "JSON_LENGTH" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("JSON_LENGTH requires 1 argument"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_LENGTH requires string argument"))?;

                let result: Int64Array = json_arr.iter()
                    .map(|opt| {
                        opt.and_then(|s| crate::json::json_length(s).ok().flatten())
                    })
                    .collect();
                Ok(Arc::new(result))
            }

            "JSON_KEYS" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("JSON_KEYS requires 1 argument"));
                }
                let json_arg = self.args[0].evaluate(batch)?;
                let json_arr = json_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("JSON_KEYS requires string argument"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if json_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        match crate::json::json_keys(json_arr.value(i)) {
                            Ok(Some(v)) => builder.append_value(&v),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            // New String Functions
            "REPLACE" => {
                if self.args.len() < 3 {
                    return Err(BlazeError::analysis("REPLACE requires 3 arguments (str, from, to)"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let from_arg = self.args[1].evaluate(batch)?;
                let to_arg = self.args[2].evaluate(batch)?;

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REPLACE requires string argument"))?;
                let from_arr = from_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REPLACE from must be string"))?;
                let to_arr = to_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REPLACE to must be string"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || from_arr.is_null(i) || to_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let result = str_arr.value(i).replace(from_arr.value(i), to_arr.value(i));
                        builder.append_value(&result);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "SUBSTRING" | "SUBSTR" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("SUBSTRING requires at least 2 arguments (str, start[, length])"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let start_arg = self.args[1].evaluate(batch)?;
                let len_arg = if self.args.len() > 2 {
                    Some(self.args[2].evaluate(batch)?)
                } else {
                    None
                };

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("SUBSTRING requires string argument"))?;
                let start_arr = start_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("SUBSTRING start must be integer"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || start_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let start = (start_arr.value(i).max(1) - 1) as usize; // SQL is 1-indexed
                        let chars: Vec<char> = s.chars().collect();

                        let result = if let Some(ref len_arr_ref) = len_arg {
                            let len_arr = len_arr_ref.as_any().downcast_ref::<Int64Array>()
                                .ok_or_else(|| BlazeError::type_error("SUBSTRING length must be integer"))?;
                            if len_arr.is_null(i) {
                                builder.append_null();
                                continue;
                            }
                            let len = len_arr.value(i).max(0) as usize;
                            chars.iter().skip(start).take(len).collect::<String>()
                        } else {
                            chars.iter().skip(start).collect::<String>()
                        };
                        builder.append_value(&result);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "LEFT" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("LEFT requires 2 arguments (str, n)"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let n_arg = self.args[1].evaluate(batch)?;

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("LEFT requires string argument"))?;
                let n_arr = n_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("LEFT n must be integer"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || n_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let n = n_arr.value(i).max(0) as usize;
                        let result: String = s.chars().take(n).collect();
                        builder.append_value(&result);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "RIGHT" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("RIGHT requires 2 arguments (str, n)"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let n_arg = self.args[1].evaluate(batch)?;

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("RIGHT requires string argument"))?;
                let n_arr = n_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("RIGHT n must be integer"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || n_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let n = n_arr.value(i).max(0) as usize;
                        let chars: Vec<char> = s.chars().collect();
                        let start = chars.len().saturating_sub(n);
                        let result: String = chars.into_iter().skip(start).collect();
                        builder.append_value(&result);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "LPAD" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("LPAD requires at least 2 arguments (str, len[, pad])"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let len_arg = self.args[1].evaluate(batch)?;
                let pad_arg = if self.args.len() > 2 {
                    Some(self.args[2].evaluate(batch)?)
                } else {
                    None
                };

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("LPAD requires string argument"))?;
                let len_arr = len_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("LPAD length must be integer"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || len_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let target_len = len_arr.value(i).max(0) as usize;
                        let pad_char = if let Some(ref pad_arr_ref) = pad_arg {
                            let pad_arr = pad_arr_ref.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| BlazeError::type_error("LPAD pad must be string"))?;
                            if pad_arr.is_null(i) || pad_arr.value(i).is_empty() {
                                " ".to_string()
                            } else {
                                pad_arr.value(i).to_string()
                            }
                        } else {
                            " ".to_string()
                        };

                        let s_len = s.chars().count();
                        let result = if s_len >= target_len {
                            s.chars().take(target_len).collect::<String>()
                        } else {
                            let pad_needed = target_len - s_len;
                            let pad_chars: Vec<char> = pad_char.chars().collect();
                            let mut padding = String::new();
                            for j in 0..pad_needed {
                                padding.push(pad_chars[j % pad_chars.len()]);
                            }
                            format!("{}{}", padding, s)
                        };
                        builder.append_value(&result);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "RPAD" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("RPAD requires at least 2 arguments (str, len[, pad])"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let len_arg = self.args[1].evaluate(batch)?;
                let pad_arg = if self.args.len() > 2 {
                    Some(self.args[2].evaluate(batch)?)
                } else {
                    None
                };

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("RPAD requires string argument"))?;
                let len_arr = len_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("RPAD length must be integer"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || len_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let target_len = len_arr.value(i).max(0) as usize;
                        let pad_char = if let Some(ref pad_arr_ref) = pad_arg {
                            let pad_arr = pad_arr_ref.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| BlazeError::type_error("RPAD pad must be string"))?;
                            if pad_arr.is_null(i) || pad_arr.value(i).is_empty() {
                                " ".to_string()
                            } else {
                                pad_arr.value(i).to_string()
                            }
                        } else {
                            " ".to_string()
                        };

                        let s_len = s.chars().count();
                        let result = if s_len >= target_len {
                            s.chars().take(target_len).collect::<String>()
                        } else {
                            let pad_needed = target_len - s_len;
                            let pad_chars: Vec<char> = pad_char.chars().collect();
                            let mut padding = String::new();
                            for j in 0..pad_needed {
                                padding.push(pad_chars[j % pad_chars.len()]);
                            }
                            format!("{}{}", s, padding)
                        };
                        builder.append_value(&result);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "REVERSE" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("REVERSE requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                let str_arr = arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REVERSE requires string argument"))?;

                let result: StringArray = str_arr.iter()
                    .map(|opt| opt.map(|s| s.chars().rev().collect::<String>()))
                    .collect();
                Ok(Arc::new(result))
            }

            "SPLIT_PART" => {
                if self.args.len() < 3 {
                    return Err(BlazeError::analysis("SPLIT_PART requires 3 arguments (str, delimiter, index)"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let delim_arg = self.args[1].evaluate(batch)?;
                let idx_arg = self.args[2].evaluate(batch)?;

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("SPLIT_PART requires string argument"))?;
                let delim_arr = delim_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("SPLIT_PART delimiter must be string"))?;
                let idx_arr = idx_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("SPLIT_PART index must be integer"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || delim_arr.is_null(i) || idx_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let delim = delim_arr.value(i);
                        let idx = idx_arr.value(i);

                        // SQL SPLIT_PART is 1-indexed
                        if idx < 1 {
                            builder.append_value("");
                        } else {
                            let parts: Vec<&str> = s.split(delim).collect();
                            let result = parts.get((idx - 1) as usize).unwrap_or(&"");
                            builder.append_value(result);
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "REGEXP_MATCH" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("REGEXP_MATCH requires 2 arguments (str, pattern)"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let pattern_arg = self.args[1].evaluate(batch)?;

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REGEXP_MATCH requires string argument"))?;
                let pattern_arr = pattern_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REGEXP_MATCH pattern must be string"))?;

                let mut builder = BooleanBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => builder.append_value(re.is_match(s)),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "REGEXP_REPLACE" => {
                if self.args.len() < 3 {
                    return Err(BlazeError::analysis("REGEXP_REPLACE requires 3 arguments (str, pattern, replacement)"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let pattern_arg = self.args[1].evaluate(batch)?;
                let replacement_arg = self.args[2].evaluate(batch)?;

                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REGEXP_REPLACE requires string argument"))?;
                let pattern_arr = pattern_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REGEXP_REPLACE pattern must be string"))?;
                let replacement_arr = replacement_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("REGEXP_REPLACE replacement must be string"))?;

                let mut builder = StringBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) || replacement_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        let replacement = replacement_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => {
                                let result = re.replace_all(s, replacement);
                                builder.append_value(&result);
                            }
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            // Date/Time extraction functions
            "YEAR" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("YEAR requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                self.extract_date_part(&arg, "YEAR", batch.num_rows())
            }

            "MONTH" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("MONTH requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                self.extract_date_part(&arg, "MONTH", batch.num_rows())
            }

            "DAY" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("DAY requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                self.extract_date_part(&arg, "DAY", batch.num_rows())
            }

            "HOUR" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("HOUR requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                self.extract_date_part(&arg, "HOUR", batch.num_rows())
            }

            "MINUTE" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("MINUTE requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                self.extract_date_part(&arg, "MINUTE", batch.num_rows())
            }

            "SECOND" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("SECOND requires 1 argument"));
                }
                let arg = self.args[0].evaluate(batch)?;
                self.extract_date_part(&arg, "SECOND", batch.num_rows())
            }

            "TO_DATE" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("TO_DATE requires at least 1 argument"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("TO_DATE requires string argument"))?;

                let format = if self.args.len() > 1 {
                    let fmt_arg = self.args[1].evaluate(batch)?;
                    let fmt_arr = fmt_arg.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| BlazeError::type_error("TO_DATE format must be string"))?;
                    if !fmt_arr.is_null(0) {
                        Some(fmt_arr.value(0).to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };

                use chrono::{NaiveDate, Datelike};
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

                let mut builder = arrow::array::Date32Builder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let parsed = if let Some(ref fmt) = format {
                            NaiveDate::parse_from_str(s, fmt).ok()
                        } else {
                            // Try common formats
                            NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
                                .or_else(|| NaiveDate::parse_from_str(s, "%Y/%m/%d").ok())
                                .or_else(|| NaiveDate::parse_from_str(s, "%d-%m-%Y").ok())
                        };

                        match parsed {
                            Some(date) => {
                                let days = date.num_days_from_ce() - epoch.num_days_from_ce();
                                builder.append_value(days);
                            }
                            None => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "TO_TIMESTAMP" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("TO_TIMESTAMP requires at least 1 argument"));
                }
                let str_arg = self.args[0].evaluate(batch)?;
                let str_arr = str_arg.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("TO_TIMESTAMP requires string argument"))?;

                let format = if self.args.len() > 1 {
                    let fmt_arg = self.args[1].evaluate(batch)?;
                    let fmt_arr = fmt_arg.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| BlazeError::type_error("TO_TIMESTAMP format must be string"))?;
                    if !fmt_arr.is_null(0) {
                        Some(fmt_arr.value(0).to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };

                use chrono::NaiveDateTime;

                let mut builder = arrow::array::TimestampMicrosecondBuilder::new();
                for i in 0..batch.num_rows() {
                    if str_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        let parsed = if let Some(ref fmt) = format {
                            NaiveDateTime::parse_from_str(s, fmt).ok()
                        } else {
                            // Try common formats
                            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()
                                .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
                                .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
                        };

                        match parsed {
                            Some(dt) => builder.append_value(dt.and_utc().timestamp_micros()),
                            None => builder.append_null(),
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }

            "DATE_ADD" | "DATEADD" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("DATE_ADD requires 2 arguments (date, days)"));
                }
                let date_arg = self.args[0].evaluate(batch)?;
                let days_arg = self.args[1].evaluate(batch)?;

                let days_arr = days_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("DATE_ADD days must be integer"))?;

                match date_arg.data_type() {
                    ArrowDataType::Date32 => {
                        let date_arr = date_arg.as_any().downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Date32"))?;
                        let result: arrow::array::Date32Array = date_arr.iter()
                            .zip(days_arr.iter())
                            .map(|(d, days)| {
                                match (d, days) {
                                    (Some(d), Some(days)) => Some(d + days as i32),
                                    _ => None,
                                }
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }
                    ArrowDataType::Timestamp(_, _) => {
                        let ts_arr = date_arg.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                            .ok_or_else(|| BlazeError::type_error("Expected Timestamp"))?;
                        let micros_per_day = 24 * 60 * 60 * 1_000_000i64;
                        let result: arrow::array::TimestampMicrosecondArray = ts_arr.iter()
                            .zip(days_arr.iter())
                            .map(|(ts, days)| {
                                match (ts, days) {
                                    (Some(ts), Some(days)) => Some(ts + days * micros_per_day),
                                    _ => None,
                                }
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }
                    _ => Err(BlazeError::type_error("DATE_ADD requires date or timestamp argument")),
                }
            }

            "DATE_SUB" | "DATESUB" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("DATE_SUB requires 2 arguments (date, days)"));
                }
                let date_arg = self.args[0].evaluate(batch)?;
                let days_arg = self.args[1].evaluate(batch)?;

                let days_arr = days_arg.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("DATE_SUB days must be integer"))?;

                match date_arg.data_type() {
                    ArrowDataType::Date32 => {
                        let date_arr = date_arg.as_any().downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Date32"))?;
                        let result: arrow::array::Date32Array = date_arr.iter()
                            .zip(days_arr.iter())
                            .map(|(d, days)| {
                                match (d, days) {
                                    (Some(d), Some(days)) => Some(d - days as i32),
                                    _ => None,
                                }
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }
                    ArrowDataType::Timestamp(_, _) => {
                        let ts_arr = date_arg.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                            .ok_or_else(|| BlazeError::type_error("Expected Timestamp"))?;
                        let micros_per_day = 24 * 60 * 60 * 1_000_000i64;
                        let result: arrow::array::TimestampMicrosecondArray = ts_arr.iter()
                            .zip(days_arr.iter())
                            .map(|(ts, days)| {
                                match (ts, days) {
                                    (Some(ts), Some(days)) => Some(ts - days * micros_per_day),
                                    _ => None,
                                }
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }
                    _ => Err(BlazeError::type_error("DATE_SUB requires date or timestamp argument")),
                }
            }

            "DATE_DIFF" | "DATEDIFF" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("DATE_DIFF requires at least 2 arguments (date1, date2[, unit])"));
                }
                let date1_arg = self.args[0].evaluate(batch)?;
                let date2_arg = self.args[1].evaluate(batch)?;

                let unit = if self.args.len() > 2 {
                    let unit_arg = self.args[2].evaluate(batch)?;
                    let unit_arr = unit_arg.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| BlazeError::type_error("DATE_DIFF unit must be string"))?;
                    if !unit_arr.is_null(0) {
                        unit_arr.value(0).to_uppercase()
                    } else {
                        "DAY".to_string()
                    }
                } else {
                    "DAY".to_string()
                };

                match (date1_arg.data_type(), date2_arg.data_type()) {
                    (ArrowDataType::Date32, ArrowDataType::Date32) => {
                        let date1_arr = date1_arg.as_any().downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Date32"))?;
                        let date2_arr = date2_arg.as_any().downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| BlazeError::type_error("Expected Date32"))?;

                        let result: Int64Array = date1_arr.iter()
                            .zip(date2_arr.iter())
                            .map(|(d1, d2)| {
                                match (d1, d2) {
                                    (Some(d1), Some(d2)) => {
                                        let diff_days = (d1 - d2) as i64;
                                        match unit.as_str() {
                                            "DAY" | "DAYS" => Some(diff_days),
                                            "WEEK" | "WEEKS" => Some(diff_days / 7),
                                            "MONTH" | "MONTHS" => Some(diff_days / 30),
                                            "YEAR" | "YEARS" => Some(diff_days / 365),
                                            _ => Some(diff_days),
                                        }
                                    }
                                    _ => None,
                                }
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }
                    _ => Err(BlazeError::type_error("DATE_DIFF requires date arguments")),
                }
            }

            // Utility Functions
            "GREATEST" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("GREATEST requires at least 1 argument"));
                }

                let first = self.args[0].evaluate(batch)?;
                let mut result = first;

                for arg_expr in self.args.iter().skip(1) {
                    let arg = arg_expr.evaluate(batch)?;
                    // Element-wise maximum using downcast
                    if let (Some(r), Some(a)) = (
                        result.as_any().downcast_ref::<Int64Array>(),
                        arg.as_any().downcast_ref::<Int64Array>()
                    ) {
                        let max_arr: Int64Array = r.iter().zip(a.iter())
                            .map(|(rv, av)| match (rv, av) {
                                (Some(x), Some(y)) => Some(x.max(y)),
                                (Some(x), None) => Some(x),
                                (None, Some(y)) => Some(y),
                                (None, None) => None,
                            })
                            .collect();
                        result = Arc::new(max_arr);
                    } else if let (Some(r), Some(a)) = (
                        result.as_any().downcast_ref::<Float64Array>(),
                        arg.as_any().downcast_ref::<Float64Array>()
                    ) {
                        let max_arr: Float64Array = r.iter().zip(a.iter())
                            .map(|(rv, av)| match (rv, av) {
                                (Some(x), Some(y)) => Some(x.max(y)),
                                (Some(x), None) => Some(x),
                                (None, Some(y)) => Some(y),
                                (None, None) => None,
                            })
                            .collect();
                        result = Arc::new(max_arr);
                    }
                }
                Ok(result)
            }

            "LEAST" => {
                if self.args.is_empty() {
                    return Err(BlazeError::analysis("LEAST requires at least 1 argument"));
                }

                let first = self.args[0].evaluate(batch)?;
                let mut result = first;

                for arg_expr in self.args.iter().skip(1) {
                    let arg = arg_expr.evaluate(batch)?;
                    // Element-wise minimum using downcast
                    if let (Some(r), Some(a)) = (
                        result.as_any().downcast_ref::<Int64Array>(),
                        arg.as_any().downcast_ref::<Int64Array>()
                    ) {
                        let min_arr: Int64Array = r.iter().zip(a.iter())
                            .map(|(rv, av)| match (rv, av) {
                                (Some(x), Some(y)) => Some(x.min(y)),
                                (Some(x), None) => Some(x),
                                (None, Some(y)) => Some(y),
                                (None, None) => None,
                            })
                            .collect();
                        result = Arc::new(min_arr);
                    } else if let (Some(r), Some(a)) = (
                        result.as_any().downcast_ref::<Float64Array>(),
                        arg.as_any().downcast_ref::<Float64Array>()
                    ) {
                        let min_arr: Float64Array = r.iter().zip(a.iter())
                            .map(|(rv, av)| match (rv, av) {
                                (Some(x), Some(y)) => Some(x.min(y)),
                                (Some(x), None) => Some(x),
                                (None, Some(y)) => Some(y),
                                (None, None) => None,
                            })
                            .collect();
                        result = Arc::new(min_arr);
                    }
                }
                Ok(result)
            }

            "IFNULL" | "NVL" => {
                if self.args.len() < 2 {
                    return Err(BlazeError::analysis("IFNULL/NVL requires 2 arguments"));
                }
                let val = self.args[0].evaluate(batch)?;
                let default = self.args[1].evaluate(batch)?;

                let is_null = arrow::compute::is_null(&val)?;
                let result = arrow::compute::kernels::zip::zip(&is_null, &default, &val)?;
                Ok(result)
            }

            _ => Err(BlazeError::not_implemented(format!("Function: {}", self.name))),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl ScalarFunctionExpr {
    /// Helper function to extract date parts from date/timestamp arrays
    fn extract_date_part(&self, arr: &ArrayRef, part: &str, num_rows: usize) -> Result<ArrayRef> {
        use chrono::{Datelike, Timelike};

        match arr.data_type() {
            ArrowDataType::Date32 => {
                let date_arr = arr.as_any().downcast_ref::<arrow::array::Date32Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Date32"))?;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

                let result: Int64Array = date_arr.iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = epoch + chrono::Duration::days(days as i64);
                            match part {
                                "YEAR" => date.year() as i64,
                                "MONTH" => date.month() as i64,
                                "DAY" => date.day() as i64,
                                _ => 0,
                            }
                        })
                    })
                    .collect();
                Ok(Arc::new(result))
            }
            ArrowDataType::Timestamp(_, _) => {
                let ts_arr = arr.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected Timestamp"))?;

                let result: Int64Array = ts_arr.iter()
                    .map(|opt| {
                        opt.map(|micros| {
                            let dt = chrono::DateTime::from_timestamp_micros(micros)
                                .map(|dt| dt.naive_utc())
                                .unwrap_or_else(chrono::NaiveDateTime::default);
                            match part {
                                "YEAR" => dt.year() as i64,
                                "MONTH" => dt.month() as i64,
                                "DAY" => dt.day() as i64,
                                "HOUR" => dt.hour() as i64,
                                "MINUTE" => dt.minute() as i64,
                                "SECOND" => dt.second() as i64,
                                _ => 0,
                            }
                        })
                    })
                    .collect();
                Ok(Arc::new(result))
            }
            _ => Err(BlazeError::type_error(format!("{} requires date or timestamp argument", part))),
        }
    }
}

/// Scalar subquery expression that holds a precomputed value.
/// The subquery is executed once and the result is stored as a scalar.
#[derive(Debug)]
pub struct ScalarSubqueryExpr {
    /// The precomputed result of the subquery
    value: ScalarValue,
    /// Name for this expression
    name: String,
}

impl ScalarSubqueryExpr {
    pub fn new(value: ScalarValue) -> Self {
        Self {
            name: "scalar_subquery".to_string(),
            value,
        }
    }
}

impl PhysicalExpr for ScalarSubqueryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        self.value.data_type().to_arrow()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // Expand the scalar value to match the batch size
        self.value.to_array_of_size(batch.num_rows())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// EXISTS subquery expression that holds a precomputed boolean result.
#[derive(Debug)]
pub struct ExistsExpr {
    /// Whether the subquery returned any rows
    exists: bool,
    /// Whether the condition is negated (NOT EXISTS)
    negated: bool,
    /// Name for this expression
    name: String,
}

impl ExistsExpr {
    pub fn new(exists: bool, negated: bool) -> Self {
        Self {
            exists,
            negated,
            name: if negated { "not_exists" } else { "exists" }.to_string(),
        }
    }
}

impl PhysicalExpr for ExistsExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let result = if self.negated {
            !self.exists
        } else {
            self.exists
        };

        // Create a boolean array with the same value for all rows
        let mut builder = BooleanBuilder::new();
        for _ in 0..batch.num_rows() {
            builder.append_value(result);
        }
        Ok(Arc::new(builder.finish()))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// IN subquery expression that checks if value is in a set.
#[derive(Debug)]
pub struct InSubqueryExpr {
    /// The expression to check
    expr: Arc<dyn PhysicalExpr>,
    /// Precomputed set of values from the subquery
    values: Vec<ScalarValue>,
    /// Whether the condition is negated (NOT IN)
    negated: bool,
    /// Name for this expression
    name: String,
}

impl InSubqueryExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, values: Vec<ScalarValue>, negated: bool) -> Self {
        Self {
            name: if negated { "not_in_subquery" } else { "in_subquery" }.to_string(),
            expr,
            values,
            negated,
        }
    }
}

impl PhysicalExpr for InSubqueryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let expr_array = self.expr.evaluate(batch)?;
        let mut builder = BooleanBuilder::new();

        for i in 0..batch.num_rows() {
            // Convert the array value to ScalarValue for comparison
            let value = ScalarValue::try_from_array(&expr_array, i)?;

            let found = self.values.iter().any(|v| v == &value);
            let result = if self.negated { !found } else { found };
            builder.append_value(result);
        }

        Ok(Arc::new(builder.finish()))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Int64, false),
            Field::new("b", ArrowDataType::Int64, false),
        ]));

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]));

        RecordBatch::try_new(schema, vec![a, b]).unwrap()
    }

    #[test]
    fn test_column_expr() {
        let batch = create_test_batch();
        let expr = ColumnExpr::new("a", 0);
        let result = expr.evaluate(&batch).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_literal_expr() {
        let batch = create_test_batch();
        let expr = LiteralExpr::new(ScalarValue::Int64(Some(42)));
        let result = expr.evaluate(&batch).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_binary_comparison() {
        let batch = create_test_batch();
        let left = Arc::new(ColumnExpr::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let right = Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(3)))) as Arc<dyn PhysicalExpr>;

        let expr = BinaryExpr::new(left, "gt", right);
        let result = expr.evaluate(&batch).unwrap();

        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bool_arr.value(0)); // 1 > 3 = false
        assert!(!bool_arr.value(1)); // 2 > 3 = false
        assert!(!bool_arr.value(2)); // 3 > 3 = false
        assert!(bool_arr.value(3));  // 4 > 3 = true
        assert!(bool_arr.value(4));  // 5 > 3 = true
    }

    #[test]
    fn test_between_expr() {
        let batch = create_test_batch();
        let expr = Arc::new(ColumnExpr::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let low = Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(2)))) as Arc<dyn PhysicalExpr>;
        let high = Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(4)))) as Arc<dyn PhysicalExpr>;

        let between = BetweenExpr::new(expr, low, high, false);
        let result = between.evaluate(&batch).unwrap();

        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bool_arr.value(0)); // 1 BETWEEN 2 AND 4 = false
        assert!(bool_arr.value(1));  // 2 BETWEEN 2 AND 4 = true
        assert!(bool_arr.value(2));  // 3 BETWEEN 2 AND 4 = true
        assert!(bool_arr.value(3));  // 4 BETWEEN 2 AND 4 = true
        assert!(!bool_arr.value(4)); // 5 BETWEEN 2 AND 4 = false
    }

    #[test]
    fn test_in_list_expr() {
        let batch = create_test_batch();
        let expr = Arc::new(ColumnExpr::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let list = vec![
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(1)))) as Arc<dyn PhysicalExpr>,
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(3)))) as Arc<dyn PhysicalExpr>,
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(5)))) as Arc<dyn PhysicalExpr>,
        ];

        let in_list = InListExpr::new(expr, list, false);
        let result = in_list.evaluate(&batch).unwrap();

        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(0));  // 1 IN (1, 3, 5) = true
        assert!(!bool_arr.value(1)); // 2 IN (1, 3, 5) = false
        assert!(bool_arr.value(2));  // 3 IN (1, 3, 5) = true
        assert!(!bool_arr.value(3)); // 4 IN (1, 3, 5) = false
        assert!(bool_arr.value(4));  // 5 IN (1, 3, 5) = true
    }

    #[test]
    fn test_case_expr() {
        let batch = create_test_batch();

        // CASE WHEN a > 3 THEN 100 ELSE 0 END
        let condition = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("a", 0)),
            "gt",
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(3)))),
        )) as Arc<dyn PhysicalExpr>;

        let then_expr = Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(100)))) as Arc<dyn PhysicalExpr>;
        let else_expr = Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(0)))) as Arc<dyn PhysicalExpr>;

        let case_expr = CaseExpr::new(
            None, // No operand (searched CASE)
            vec![(condition, then_expr)],
            Some(else_expr),
        );

        let result = case_expr.evaluate(&batch).unwrap();
        let int_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_arr.value(0), 0);   // a=1, not > 3
        assert_eq!(int_arr.value(1), 0);   // a=2, not > 3
        assert_eq!(int_arr.value(2), 0);   // a=3, not > 3
        assert_eq!(int_arr.value(3), 100); // a=4, > 3
        assert_eq!(int_arr.value(4), 100); // a=5, > 3
    }

    fn create_string_batch() -> RecordBatch {
        use arrow::array::StringArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
        ]));

        let names: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]));

        RecordBatch::try_new(schema, vec![names]).unwrap()
    }

    #[test]
    fn test_upper_function() {
        let batch = create_string_batch();

        let arg = Arc::new(ColumnExpr::new("name", 0)) as Arc<dyn PhysicalExpr>;
        let upper_fn = ScalarFunctionExpr::new("UPPER", vec![arg]);

        let result = upper_fn.evaluate(&batch).unwrap();
        let str_arr = result.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();

        assert_eq!(str_arr.value(0), "ALICE");
        assert_eq!(str_arr.value(1), "BOB");
        assert_eq!(str_arr.value(2), "CHARLIE");
    }

    #[test]
    fn test_lower_function() {
        use arrow::array::StringArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let names: ArrayRef = Arc::new(StringArray::from(vec!["HELLO", "WORLD"]));
        let batch = RecordBatch::try_new(schema, vec![names]).unwrap();

        let arg = Arc::new(ColumnExpr::new("name", 0)) as Arc<dyn PhysicalExpr>;
        let lower_fn = ScalarFunctionExpr::new("LOWER", vec![arg]);

        let result = lower_fn.evaluate(&batch).unwrap();
        let str_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(str_arr.value(0), "hello");
        assert_eq!(str_arr.value(1), "world");
    }

    #[test]
    fn test_abs_function() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", ArrowDataType::Int64, false),
        ]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![-5, -3, 0, 3, 5]));
        let batch = RecordBatch::try_new(schema, vec![vals]).unwrap();

        let arg = Arc::new(ColumnExpr::new("val", 0)) as Arc<dyn PhysicalExpr>;
        let abs_fn = ScalarFunctionExpr::new("ABS", vec![arg]);

        let result = abs_fn.evaluate(&batch).unwrap();
        let int_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_arr.value(0), 5);
        assert_eq!(int_arr.value(1), 3);
        assert_eq!(int_arr.value(2), 0);
        assert_eq!(int_arr.value(3), 3);
        assert_eq!(int_arr.value(4), 5);
    }
}
