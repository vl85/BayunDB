// Expression Evaluation Utility

use crate::query::parser::ast::{Expression, Value, Operator};
use crate::query::executor::result::{Row, DataValue, QueryResult, QueryError};
use crate::catalog::table::Table; // Assuming Row doesn't embed full schema info

// Evaluate an AST expression in the context of a single row.
// Requires the table schema to resolve column types and potentially for future context.
pub fn evaluate_expression(expr: &Expression, row: &Row, table_schema: Option<&Table>) -> QueryResult<DataValue> {
    match expr {
        Expression::Literal(val) => {
            match val {
                Value::Integer(i) => Ok(DataValue::Integer(*i)),
                Value::Float(f) => Ok(DataValue::Float(*f)),
                Value::String(s) => Ok(DataValue::Text(s.clone())),
                Value::Boolean(b) => Ok(DataValue::Boolean(*b)),
                Value::Null => Ok(DataValue::Null),
            }
        }
        Expression::Column(col_ref) => {
            let logical_col_name = &col_ref.name;

            // 1. Try direct lookup by logical_col_name (e.g., if row keys are unqualified or already match a specific alias.logical_col_name form)
            if let Some(value) = row.get(logical_col_name) {
                return Ok(value.clone());
            }

            if let Some(table_schema) = table_schema {
                // 2. Construct a qualified name based on AST table reference or schema table name
                let qualified_name_attempt = match &col_ref.table {
                    Some(ast_table_name) => format!("{}.{}", ast_table_name, logical_col_name),
                    None => format!("{}.{}", table_schema.name(), logical_col_name), // e.g., "derived_input_schema.id"
                };

                if let Some(value) = row.get(&qualified_name_attempt) {
                    return Ok(value.clone());
                }

                // 3. Fallback for unqualified logical names (col_ref.table is None)
                //    The row might contain keys like "actual_table.logical_col_name".
                //    This is especially relevant if `table_schema.name()` was a placeholder like "derived_input_schema".
                if col_ref.table.is_none() {
                    for physical_col_name_in_row_key in row.columns() { // These are the keys in the row's HashMap
                        // Check if physical_col_name_in_row_key is "some_prefix.logical_col_name"
                        if physical_col_name_in_row_key.ends_with(&format!(".{}", logical_col_name)) {
                            // Heuristic: if it ends with the desired logical name, take it.
                            // This could be ambiguous if multiple tables had same suffix, but less likely with scan outputs.
                            if let Some(value) = row.get(physical_col_name_in_row_key) {
                                return Ok(value.clone());
                            }
                        }
                    }
                }
                
                // If all attempts fail, report specific error including the schema name used for qualification
                Err(QueryError::ColumnNotFound(format!(
                    "Column '{}' (logical), attempted qualified name '{}', not found in row {:?} using schema table name '{}'",
                    logical_col_name, qualified_name_attempt, row, table_schema.name()
                )))
            } else {
                // No schema provided.
                // Step 1 (direct lookup) failed already.
                // Try the fallback heuristic (Step 3 from the `if let Some(table_schema)` block)
                if col_ref.table.is_none() { // Only try this if the requested column was unqualified
                    for physical_col_name_in_row_key in row.columns() {
                        if physical_col_name_in_row_key.ends_with(&format!(".{}", logical_col_name)) {
                            if let Some(value) = row.get(physical_col_name_in_row_key) {
                                return Ok(value.clone());
                            }
                        }
                    }
                }
                // If the heuristic also fails, report the error.
                Err(QueryError::ColumnNotFound(format!(
                    "Column '{}' not found in row {:?} (no schema provided for qualification attempts)",
                    logical_col_name, row
                )))
            }
        }
        Expression::BinaryOp { left, op, right } => {
            // Note: table_schema is passed down recursively.
            let left_val = evaluate_expression(left, row, table_schema)?;
            let right_val = evaluate_expression(right, row, table_schema)?;

            // Handle NULL propagation: typically, op(NULL, _) -> NULL, op(_, NULL) -> NULL
            if left_val == DataValue::Null || right_val == DataValue::Null {
                // Exceptions: IS NULL, IS NOT NULL (handled separately or maybe here?)
                 // For now, basic propagation
                 return Ok(DataValue::Null);
            }

            match op {
                Operator::Equals => Ok(DataValue::Boolean(left_val == right_val)),
                Operator::NotEquals => Ok(DataValue::Boolean(left_val != right_val)),
                Operator::LessThan => {
                    match left_val.partial_cmp(&right_val) {
                        Some(std::cmp::Ordering::Less) => Ok(DataValue::Boolean(true)),
                        Some(_) => Ok(DataValue::Boolean(false)),
                        None => Err(QueryError::TypeError(format!("Cannot compare {:?} < {:?}", left_val, right_val))), // Incompatible types
                    }
                }
                Operator::GreaterThan => {
                    match left_val.partial_cmp(&right_val) {
                        Some(std::cmp::Ordering::Greater) => Ok(DataValue::Boolean(true)),
                        Some(_) => Ok(DataValue::Boolean(false)),
                        None => Err(QueryError::TypeError(format!("Cannot compare {:?} > {:?}", left_val, right_val))),
                    }
                }
                Operator::LessEquals => {
                    match left_val.partial_cmp(&right_val) {
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => Ok(DataValue::Boolean(true)),
                        Some(_) => Ok(DataValue::Boolean(false)),
                         None => Err(QueryError::TypeError(format!("Cannot compare {:?} <= {:?}", left_val, right_val))),
                    }
                }
                Operator::GreaterEquals => {
                    match left_val.partial_cmp(&right_val) {
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => Ok(DataValue::Boolean(true)),
                        Some(_) => Ok(DataValue::Boolean(false)),
                         None => Err(QueryError::TypeError(format!("Cannot compare {:?} >= {:?}", left_val, right_val))),
                    }
                }
                Operator::Plus => {
                    match (left_val, right_val) {
                        (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Integer(l.checked_add(r).ok_or_else(|| QueryError::NumericOverflow)?)),
                        (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l + r)), // Consider checking for float overflow/infinity?
                        (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(l as f64 + r)),
                        (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l + r as f64)),
                        _ => Err(QueryError::TypeError("Unsupported types for + operator".to_string()))
                    }
                }
                 Operator::Minus => {
                    match (left_val, right_val) {
                        (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Integer(l.checked_sub(r).ok_or_else(|| QueryError::NumericOverflow)?)),
                        (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l - r)),
                        (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(l as f64 - r)),
                        (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l - r as f64)),
                        _ => Err(QueryError::TypeError("Unsupported types for - operator".to_string()))
                    }
                }
                 Operator::Multiply => {
                    match (left_val, right_val) {
                        (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Integer(l.checked_mul(r).ok_or_else(|| QueryError::NumericOverflow)?)),
                        (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l * r)),
                        (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(l as f64 * r)),
                        (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l * r as f64)),
                        _ => Err(QueryError::TypeError("Unsupported types for * operator".to_string()))
                    }
                }
                 Operator::Divide => {
                    match (left_val, right_val) {
                        // Integer division
                        (DataValue::Integer(l), DataValue::Integer(r)) => {
                             if r == 0 { Err(QueryError::DivisionByZero) }
                             else { Ok(DataValue::Integer(l / r)) } // Note: truncates
                        }
                        // Float division
                        (DataValue::Float(l), DataValue::Float(r)) => {
                             if r == 0.0 { Err(QueryError::DivisionByZero) }
                             else { Ok(DataValue::Float(l / r)) }
                        }
                         (DataValue::Integer(l), DataValue::Float(r)) => {
                             if r == 0.0 { Err(QueryError::DivisionByZero) }
                             else { Ok(DataValue::Float(l as f64 / r)) }
                        }
                         (DataValue::Float(l), DataValue::Integer(r)) => {
                             if r == 0 { Err(QueryError::DivisionByZero) }
                             else { Ok(DataValue::Float(l / r as f64)) }
                        }
                        _ => Err(QueryError::TypeError("Unsupported types for / operator".to_string()))
                    }
                }
                Operator::Modulo => {
                     match (left_val, right_val) {
                        (DataValue::Integer(l), DataValue::Integer(r)) => {
                             if r == 0 { Err(QueryError::DivisionByZero) } // Modulo by zero is error
                             else { Ok(DataValue::Integer(l % r)) }
                        }
                         // Modulo typically not defined for floats in SQL? Or depends on dialect.
                         // Returning error for now.
                        _ => Err(QueryError::TypeError("Modulo operator only supports integers".to_string()))
                     }
                }
                // Logical Operators (handle NULL propagation implicitly handled above)
                 Operator::And => {
                     match (left_val, right_val) {
                         (DataValue::Boolean(l), DataValue::Boolean(r)) => Ok(DataValue::Boolean(l && r)),
                         _ => Err(QueryError::TypeError("AND requires boolean operands".to_string()))
                     }
                 }
                 Operator::Or => {
                     match (left_val, right_val) {
                         (DataValue::Boolean(l), DataValue::Boolean(r)) => Ok(DataValue::Boolean(l || r)),
                         _ => Err(QueryError::TypeError("OR requires boolean operands".to_string()))
                     }
                 }
                _ => Err(QueryError::InvalidOperation(format!("Unsupported binary operator {:?} for evaluation", op)))
            }
        }
        Expression::UnaryOp { op, expr } => {
            let val = evaluate_expression(expr, row, table_schema)?;
            match op {
                crate::query::parser::ast::UnaryOperator::Minus => {
                    match val {
                        DataValue::Integer(i) => Ok(DataValue::Integer(-i)), // Consider checked_neg?
                        DataValue::Float(f) => Ok(DataValue::Float(-f)),
                        DataValue::Null => Ok(DataValue::Null), // -NULL is NULL
                        _ => Err(QueryError::TypeError(format!("Unary minus not supported for type {:?}", val.get_type())))
                    }
                }
                crate::query::parser::ast::UnaryOperator::Not => {
                     // Three-valued logic for NOT: NOT TRUE -> FALSE, NOT FALSE -> TRUE, NOT NULL -> NULL
                    match val {
                        DataValue::Boolean(b) => Ok(DataValue::Boolean(!b)),
                        DataValue::Null => Ok(DataValue::Null),
                        _ => Err(QueryError::TypeError(format!("Unary NOT requires a boolean or NULL operand, got {:?}", val.get_type())))
                    }
                }
            }
        }
        Expression::Case { operand, when_then_clauses, else_clause } => {
             let operand_value = match operand {
                Some(op_expr) => Some(evaluate_expression(op_expr, row, table_schema)?),
                None => None,
            };

            for (when_expr, then_expr) in when_then_clauses {
                let condition_met = match &operand_value {
                    Some(op_val) => { // Simple CASE: CASE operand WHEN value THEN result
                        let current_when_value = evaluate_expression(when_expr, row, table_schema)?;
                         // Handle NULL comparison: NULL = NULL is NULL (false) in standard SQL
                         if op_val == &DataValue::Null || current_when_value == DataValue::Null {
                            false
                         } else {
                            op_val == &current_when_value
                         }
                    }
                    None => { // Searched CASE: CASE WHEN condition THEN result
                        let condition_result = evaluate_expression(when_expr, row, table_schema)?;
                        match condition_result {
                            DataValue::Boolean(b) => b,
                            // SQL standard: NULL in condition evaluates to false
                            DataValue::Null => false,
                            _ => return Err(QueryError::TypeError("CASE WHEN condition did not evaluate to a boolean or NULL".to_string())),
                        }
                    }
                };

                if condition_met {
                    // Evaluate and return the corresponding THEN expression
                    return evaluate_expression(then_expr, row, table_schema);
                }
            }

            // No WHEN clause matched, evaluate the ELSE clause or return NULL
            if let Some(else_expr) = else_clause {
                evaluate_expression(else_expr, row, table_schema)
            } else {
                Ok(DataValue::Null)
            }
        }
         Expression::IsNull { expr, not } => {
            let val = evaluate_expression(expr, row, table_schema)?;
            let result = val == DataValue::Null;
            Ok(DataValue::Boolean(if *not { !result } else { result }))
        }
        Expression::Aggregate { .. } => {
            Err(QueryError::ExecutionError(
                "Aggregate expressions cannot be directly evaluated. They should be pre-computed and accessed via column references.".to_string()
            ))
        }
        // TODO: Implement Function calls (would need catalog access?)
        // Expression::Function { name, args } => { ... }
        // TODO: Aggregates cannot be evaluated on a single row context. Should error here?
        // Expression::Aggregate { .. } => { ... }
        _ => Err(QueryError::ExecutionError(format!("Unsupported expression type {:?} for single row evaluation", expr)))
    }
} 