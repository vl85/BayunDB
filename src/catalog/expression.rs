use crate::catalog::schema::DataType;
use crate::catalog::table::Table;
use crate::query::parser::ast::{Expression, Value, Operator};
use crate::catalog::{ValidationError, ValidationResult};
use crate::catalog::compatibility::check_type_compatibility;

pub fn get_expression_type(expr: &Expression, table: &Table) -> ValidationResult<DataType> {
    match expr {
        Expression::Literal(value) => {
            match value {
                Value::Null => Err(ValidationError::TypeMismatch { 
                    expected: "non-null type".to_string(), 
                    actual: "NULL".to_string() 
                }),
                Value::Integer(_) => Ok(DataType::Integer),
                Value::Float(_) => Ok(DataType::Float),
                Value::String(_) => Ok(DataType::Text),
                Value::Boolean(_) => Ok(DataType::Boolean),
            }
        },
        Expression::Column(col_ref) => {
            let column = table.get_column(&col_ref.name)
                .ok_or_else(|| ValidationError::ColumnNotFound(col_ref.name.clone()))?;
            Ok(column.data_type().clone())
        },
        Expression::BinaryOp { left, op, right } => {
            let left_type = get_expression_type(left, table)?;
            let right_type = get_expression_type(right, table)?;
            check_type_compatibility(&left_type, op, &right_type)
        },
        Expression::Function { name, args } => {
            match name.to_uppercase().as_str() {
                "UPPER" | "LOWER" => {
                    if args.len() != 1 {
                        return Err(ValidationError::InvalidOperation(
                            name.clone(),
                            "expected 1 argument".to_string(),
                            format!("got {}", args.len())
                        ));
                    }
                    let arg_type = get_expression_type(&args[0], table)?;
                    if arg_type != DataType::Text {
                        return Err(ValidationError::TypeMismatch { 
                            expected: "TEXT".to_string(), 
                            actual: format!("{:?}", arg_type) 
                        });
                    }
                    Ok(DataType::Text)
                },
                "ABS" => {
                    if args.len() != 1 {
                        return Err(ValidationError::InvalidOperation(
                            name.clone(),
                            "expected 1 argument".to_string(),
                            format!("got {}", args.len())
                        ));
                    }
                    let arg_type = get_expression_type(&args[0], table)?;
                    match arg_type {
                        DataType::Integer => Ok(DataType::Integer),
                        DataType::Float => Ok(DataType::Float),
                        _ => Err(ValidationError::TypeMismatch { 
                            expected: "numeric type".to_string(), 
                            actual: format!("{:?}", arg_type) 
                        }),
                    }
                },
                _ => Err(ValidationError::InvalidOperation(
                    name.clone(),
                    "unknown function".to_string(),
                    "".to_string()
                )),
            }
        },
        Expression::Aggregate { function, arg } => {
            match function {
                crate::query::parser::ast::AggregateFunction::Count => Ok(DataType::Integer),
                _ => {
                    if let Some(arg_expr) = arg {
                        get_expression_type(arg_expr, table)
                    } else {
                        Err(ValidationError::InvalidOperation(
                            format!("{:?}", function),
                            "requires an argument".to_string(),
                            "".to_string()
                        ))
                    }
                }
            }
        },
        Expression::UnaryOp { op, expr } => {
            let expr_type = get_expression_type(expr, table)?;
            match op {
                crate::query::parser::ast::UnaryOperator::Minus => {
                    if expr_type == DataType::Integer || expr_type == DataType::Float {
                        Ok(expr_type)
                    } else {
                        Err(ValidationError::IncompatibleTypes(
                            "-".to_string(), 
                            format!("{:?}", expr_type), 
                            "(unary minus operand)".to_string()
                        ))
                    }
                }
                crate::query::parser::ast::UnaryOperator::Not => {
                    if expr_type == DataType::Boolean {
                        Ok(DataType::Boolean)
                    } else {
                        Err(ValidationError::IncompatibleTypes(
                            "NOT".to_string(), 
                            format!("{:?}", expr_type), 
                            "(unary NOT operand)".to_string()
                        ))
                    }
                }
            }
        }
        Expression::Case { when_then_clauses, else_clause, .. } => {
            if let Some((_, first_then_expr)) = when_then_clauses.first() {
                get_expression_type(first_then_expr, table)
            } else if let Some(else_expr) = else_clause {
                get_expression_type(else_expr, table)
            } else {
                let mut resolved_type: Option<DataType> = None;
                for (_, then_expr) in when_then_clauses {
                    match get_expression_type(then_expr, table) {
                        Ok(dt) => {
                            resolved_type = Some(dt);
                            break;
                        }
                        Err(_) => {}
                    }
                }
                if resolved_type.is_none() {
                    if let Some(el_expr) = else_clause {
                         match get_expression_type(el_expr, table) {
                            Ok(dt) => resolved_type = Some(dt),
                            Err(_) => {}
                        }
                    }
                }
                resolved_type.ok_or_else(|| ValidationError::TypeMismatch {
                    expected: "a determinable type from THEN/ELSE clauses".to_string(),
                    actual: "all THEN/ELSE clauses failed to resolve type or were NULL".to_string(),
                })
            }
        }
        Expression::IsNull { .. } => {
            Ok(DataType::Boolean)
        }
    }
} 