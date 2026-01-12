//! Physical expressions for query execution.

use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch,
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
        use arrow::array::{new_null_array, StringArray};
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
            "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "CONCAT" => ArrowDataType::Utf8,
            "LENGTH" | "CHAR_LENGTH" => ArrowDataType::Int64,
            "ABS" | "CEIL" | "FLOOR" | "ROUND" => {
                if !self.args.is_empty() {
                    self.args[0].data_type()
                } else {
                    ArrowDataType::Float64
                }
            }
            "COALESCE" | "NULLIF" => {
                if !self.args.is_empty() {
                    self.args[0].data_type()
                } else {
                    ArrowDataType::Null
                }
            }
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

            _ => Err(BlazeError::not_implemented(format!("Function: {}", self.name))),
        }
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
