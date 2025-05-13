// Hash-based Aggregation Operator
//
// This operator implements aggregation using a hash table to group rows

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use linked_hash_map::LinkedHashMap;
use crate::query::executor::result::{Row, QueryResult, DataValue, QueryError};
use crate::query::executor::operators::Operator;
use super::AggregateType;
use crate::query::parser::ast::Expression;
use crate::catalog::Table;
use crate::query::planner::physical_plan::PhysicalSelectExpression;
use crate::catalog::{Column, DataType as CatalogDataType};
use crate::query::executor::expression_eval;
use crate::query::parser::ast; // Added import
 // Add specific import
use std::collections::HashSet; // Add import

/// Aggregate value calculation helper struct
#[derive(Debug, Clone)]
struct AggregateValue {
    /// Type of aggregation
    agg_type: AggregateType,
    /// Count of records in the group (used for COUNT(*))
    count: i64,
    /// Count of non-NULL values that contributed to sum (used for AVG)
    value_count_for_avg_sum: i64,
    /// Sum value for SUM/AVG
    sum: Option<DataValue>,
    /// Min value for MIN
    min: Option<DataValue>,
    /// Max value for MAX
    max: Option<DataValue>,
    /// Argument for the aggregation (AST)
    arg_ast: Option<crate::query::parser::ast::Expression>,
    /// Current value of the aggregation
    current_value: DataValue,
    /// Internal lookup key, e.g. "AVG(price)"
    internal_lookup_key: String,
}

impl AggregateValue {
    /// Create a new aggregate value from an AggregateExpression descriptor
    fn new(agg_type: AggregateType, argument: Option<Expression>, internal_lookup_key: String) -> Self {
        let initial_sum = match agg_type {
            AggregateType::Sum | AggregateType::Avg => Some(DataValue::Integer(0)),
            _ => None,
        };
        let initial_value = match agg_type {
            AggregateType::Count => DataValue::Integer(0),
            _ => DataValue::Null, // Min/Max/Sum/Avg start as Null until first value
        };

        AggregateValue {
            agg_type,
            count: 0,
            value_count_for_avg_sum: 0,
            sum: initial_sum,
            min: None,
            max: None,
            arg_ast: argument,
            current_value: initial_value,
            internal_lookup_key,
        }
    }

    /// Update the aggregate with a new value from a row
    fn update(&mut self, row: &Row, input_schema: &Table) -> QueryResult<()> {
        let value = match &self.arg_ast {
            Some(expr_ast) => expression_eval::evaluate_expression(expr_ast, row, Some(input_schema))?,
            None => DataValue::Integer(1), // For COUNT(*), each row contributes 1
        };

        // Skip NULL values for SUM, AVG, MIN, MAX as per SQL standard
        if value == DataValue::Null && self.agg_type != AggregateType::Count {
            return Ok(());
        }

        match self.agg_type {
            AggregateType::Count => {
                if self.arg_ast.is_none() || value != DataValue::Null { // COUNT(*) or COUNT(non-null_expr)
                    self.count += 1;
                    self.current_value = DataValue::Integer(self.count); // Update current_value for Count
                }
            }
            AggregateType::Sum => {
                self.value_count_for_avg_sum += 1; // Also count for sum, used for consistency if AVG is also present
                match (&mut self.current_value, &value) {
                    (DataValue::Null, val) => self.current_value = val.clone(), // First non-null value
                    (DataValue::Integer(current), DataValue::Integer(new)) => {
                        *current = current.checked_add(*new).ok_or(QueryError::NumericOverflow)?;
                    }
                    (DataValue::Float(current), DataValue::Float(new)) => {
                        *current += new;
                    }
                    (DataValue::Integer(current), DataValue::Float(new)) => {
                        self.current_value = DataValue::Float(*current as f64 + new);
                    }
                    (DataValue::Float(current), DataValue::Integer(new)) => {
                        *current += *new as f64;
                    }
                    _ => return Err(QueryError::TypeError(format!("Cannot SUM incompatible types {:?} and {:?}", self.current_value, value))),
                }
            }
            AggregateType::Avg => {
                if value == DataValue::Null { return Ok(()); } // Skip nulls for AVG
                self.value_count_for_avg_sum += 1;

                match (&mut self.sum, &value) {
                    (Some(DataValue::Integer(current_sum)), DataValue::Integer(new_val)) => {
                        *current_sum = current_sum.checked_add(*new_val).ok_or(QueryError::NumericOverflow)?;
                    }
                    (Some(DataValue::Float(current_sum)), DataValue::Float(new_val)) => {
                        *current_sum += new_val;
                    }
                    (Some(DataValue::Integer(current_sum)), DataValue::Float(new_val)) => {
                        self.sum = Some(DataValue::Float(*current_sum as f64 + new_val));
                    }
                    (Some(DataValue::Float(current_sum)), DataValue::Integer(new_val)) => {
                        *current_sum += *new_val as f64;
                    }
                    (None, DataValue::Integer(new_val)) => { // First value
                        self.sum = Some(DataValue::Integer(*new_val));
                    }
                    (None, DataValue::Float(new_val)) => { // First value
                        self.sum = Some(DataValue::Float(*new_val));
                    }
                    _ => return Err(QueryError::TypeError(format!("Cannot AVG incompatible types {:?} and {:?}", self.sum, value))),
                }
            }
            AggregateType::Min => {
                if self.current_value == DataValue::Null || value < self.current_value {
                    self.current_value = value.clone();
                }
            }
            AggregateType::Max => {
                if self.current_value == DataValue::Null || value > self.current_value {
                    self.current_value = value.clone();
                }
            }
        }
        Ok(())
    }

    /// Get the final aggregate value
    fn get_result(&self) -> DataValue {
        match self.agg_type {
            AggregateType::Count => DataValue::Integer(self.count),
            AggregateType::Sum => self.current_value.clone(),
            AggregateType::Avg => {
                match &self.sum {
                    Some(sum_val) => {
                        if self.value_count_for_avg_sum > 0 {
                            let numeric_sum = match sum_val {
                                DataValue::Integer(i) => *i as f64,
                                DataValue::Float(f) => *f,
                                _ => return DataValue::Null, // Should not happen if update logic is correct
                            };
                            DataValue::Float(numeric_sum / self.value_count_for_avg_sum as f64)
                        } else {
                            DataValue::Null // Avoid division by zero, AVG of no values is NULL
                        }
                    },
                    None => DataValue::Null // No sum computed (e.g., only NULL inputs)
                }
            }
            AggregateType::Min => self.current_value.clone(),
            AggregateType::Max => self.current_value.clone(),
        }
    }
}

/// Key for the grouping hash table - combination of values from GROUP BY columns
type GroupKey = Vec<DataValue>;

/// Helper for parsing aggregate expressions AND storing final output names
#[derive(Debug, Clone)]
struct AggregateExpression {
    agg_type: AggregateType,
    argument: Option<Expression>,
    internal_lookup_key: String, // e.g., "SUM(price)" or "COUNT(*)"
    // We don't need final_output_name here, that's handled by output_select_list
}

impl AggregateExpression {
    // Helper to create the internal key and parse from AST
    fn from_ast(expr: &ast::Expression) -> QueryResult<Self> {
        if let ast::Expression::Aggregate { function, arg } = expr {
            let agg_type = AggregateType::from_ast_function(function)?;
            let arg_ast_clone = arg.as_ref().map(|boxed_expr| (**boxed_expr).clone());
            let internal_key_arg_str = arg_ast_clone.as_ref().map_or("*".to_string(), |e| e.to_string());
            let internal_lookup_key = format!("{:?}({})", agg_type, internal_key_arg_str);
            Ok(AggregateExpression {
                agg_type,
                argument: arg_ast_clone,
                internal_lookup_key,
            })
        } else {
            Err(QueryError::ExecutionError(format!("Expected Aggregate expression, found {:?}", expr)))
        }
    }
}

// In-memory group structure to hold input data during processing
#[derive(Debug, Clone)]
struct Group {
    aggregates: Vec<AggregateValue>,
    group_key_values: Vec<DataValue>,
    group_by_expressions_ast: Vec<crate::query::parser::ast::Expression>,
}

impl Group {
    fn new(key: Vec<DataValue>, aggregate_defs: &[AggregateExpression], group_exprs: Vec<Expression>) -> Self {
        let initial_aggregates = aggregate_defs.iter().map(|agg_def| {
            AggregateValue::new(agg_def.agg_type.clone(), agg_def.argument.clone(), agg_def.internal_lookup_key.clone()) 
        }).collect();
        Group {
            group_key_values: key,
            aggregates: initial_aggregates,
            group_by_expressions_ast: group_exprs,
        }
    }

    fn update(&mut self, row: &Row, input_schema: &Table) -> QueryResult<()> {
        for agg_value in &mut self.aggregates {
            agg_value.update(row, input_schema)?;
        }
        Ok(())
    }

    fn to_row(&self, output_select_list: &[PhysicalSelectExpression]) -> QueryResult<Row> {
        let mut final_row_map = LinkedHashMap::new();
        let mut final_column_order = Vec::new();
        let mut used_aggregate_indices = std::collections::HashSet::new();

        for select_expr in output_select_list {
            final_column_order.push(select_expr.output_name.clone());

            match &select_expr.expression {
                Expression::Aggregate { .. } => {
                    let mut found_agg_for_select_expr = false;
                    // Match by trying to reconstruct the internal key from select_expr's aggregate AST
                    // This assumes select_expr.expression *is* an Aggregate AST node.
                    let target_internal_key = AggregateExpression::from_ast(&select_expr.expression)
                                                .map(|ae| ae.internal_lookup_key)
                                                .unwrap_or_default(); // Fallback if parsing fails, though it shouldn't for valid agg AST

                    for (agg_idx, agg_value) in self.aggregates.iter().enumerate() {
                        if agg_value.internal_lookup_key == target_internal_key && !used_aggregate_indices.contains(&agg_idx) {
                            let result = agg_value.get_result(); // get_result now returns DataValue directly
                            final_row_map.insert(select_expr.output_name.clone(), result);
                            used_aggregate_indices.insert(agg_idx);
                            found_agg_for_select_expr = true;
                            break;
                        }
                    }
                    if !found_agg_for_select_expr {
                        // If not found by key, try by order (less robust, but as a fallback for simple cases)
                        // This part is tricky if SELECT list aggregates are not in the same order as `aggregates_for_computation`
                        // For now, let's assume a direct mapping might be needed if the key lookup failed
                        // This typically means the internal_lookup_key generation or matching logic has a subtle difference.
                        // Let's prioritize the key-based match and error if it fails robustly.
                        return Err(QueryError::ExecutionError(format!(
                            "Could not find matching aggregate result for output column '{}' (key: '{}'). Available keys: {:?}", 
                            select_expr.output_name, 
                            target_internal_key,
                            self.aggregates.iter().map(|a| &a.internal_lookup_key).collect::<Vec<_>>()
                        )));
                    }
                }
                group_key_expr_ast => { // Not an aggregate, must be a group key
                    if let Some(group_key_idx) = self.group_by_expressions_ast.iter().position(|ast| ast == group_key_expr_ast) {
                        if let Some(key_value) = self.group_key_values.get(group_key_idx) {
                            final_row_map.insert(select_expr.output_name.clone(), key_value.clone());
                        } else {
                            return Err(QueryError::ExecutionError(format!("Group key value index mismatch for output column '{}'", select_expr.output_name)));
                        }
                    } else {
                        return Err(QueryError::ExecutionError(format!("Output column '{}' ({:?}) is neither an aggregate nor a recognized group key. Group keys: {:?}", 
                            select_expr.output_name, group_key_expr_ast, self.group_by_expressions_ast)));
                    }
                }
            }
        }

        if final_row_map.len() != final_column_order.len() {
            return Err(QueryError::ExecutionError("Internal error: Mismatch between ordered columns and final map size in Group::to_row".to_string()));
        }
        Ok(Row::from_ordered_map(final_row_map, final_column_order))
    }

    // New method to create a row suitable for HAVING clause evaluation
    fn to_row_for_having_evaluation(&self) -> QueryResult<Row> {
        let mut having_row_map = LinkedHashMap::new();
        let mut having_column_order = Vec::new();

        // 1. Add group key values
        // We need names for these. Using the string representation of the group_by AST expression as a simple unique key.
        for (i, key_val) in self.group_key_values.iter().enumerate() {
            if let Some(expr_ast) = self.group_by_expressions_ast.get(i) {
                // Using a debug-like representation for the key name to ensure uniqueness for complex expressions
                let key_name = format!("{:?}", expr_ast);
                if !having_row_map.contains_key(&key_name) { // Avoid duplicates if ASTs are identical (though unlikely for group keys)
                    having_column_order.push(key_name.clone());
                    having_row_map.insert(key_name, key_val.clone());
                }
            } else {
                // This case should ideally not happen if data structures are consistent
                return Err(QueryError::ExecutionError(format!(
                    "Mismatch between group_key_values and group_by_expressions_ast at index {}", i
                )));
            }
        }

        // 2. Add all computed aggregate values using their internal_lookup_key
        for agg_value in &self.aggregates {
            let agg_key_name = agg_value.internal_lookup_key.clone();
            // Ensure we don't overwrite group keys if there's a name collision (unlikely but good to be safe)
            if !having_row_map.contains_key(&agg_key_name) {
                 having_column_order.push(agg_key_name.clone());
            }
            // Overwrite if it was a group key with the same name (less likely) or it's a new aggregate.
            having_row_map.insert(agg_key_name, agg_value.get_result());
        }
        
        Ok(Row::from_ordered_map(having_row_map, having_column_order))
    }
}

/// HashAggregateOperator
pub struct HashAggregateOperator {
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    group_by_expressions_ast: Vec<Expression>,
    having_expression: Option<Expression>,
    output_select_list: Vec<PhysicalSelectExpression>,
    aggregates_for_computation: Vec<AggregateExpression>, // Store parsed aggregates
    
    groups: HashMap<Vec<DataValue>, Group>,
    input_schema: Option<Arc<Table>>,
    processed_all_input: bool, 
    output_rows_iter: Option<Box<dyn Iterator<Item = Row> + Send + Sync>>,
}

impl Operator for HashAggregateOperator {
    fn init(&mut self) -> QueryResult<()> {
        if !self.processed_all_input {
            self.process_input_fully()?;
        }
        Ok(())
    }

    fn next(&mut self) -> QueryResult<Option<Row>> {
        // 1. Ensure all input is processed and groups are populated IF output iterator isn't set yet.
        if self.output_rows_iter.is_none() {
             self.process_input_fully()?;

            // 2. Iterate through computed groups, apply HAVING filter, and create final iterator.
            let mut final_rows = Vec::new();
            for group in self.groups.values() {
                // This row is for the SELECT list projection
                let output_row_for_select = group.to_row(&self.output_select_list)?;

                let mut passes_having = true;
                if let Some(having_expr) = &self.having_expression {
                    // This row is specifically for evaluating the HAVING clause
                    let row_for_having_eval = group.to_row_for_having_evaluation()?;

                    let transformed_having_expr = match Self::transform_expression_for_having(
                        having_expr,
                    ) {
                        Ok(expr) => expr,
                        Err(e) => return Err(e), // Propagate transformation errors
                    };

                    let having_result = expression_eval::evaluate_expression(
                        &transformed_having_expr, 
                        &row_for_having_eval, // Use the comprehensive row for HAVING
                        self.input_schema.as_deref()
                    )?;
                    passes_having = matches!(having_result, DataValue::Boolean(true)); 
                }

                if passes_having {
                    final_rows.push(output_row_for_select); // Push the row meant for final output
                }
            }
            // Store the final iterator
            self.output_rows_iter = Some(Box::new(final_rows.into_iter())); 
        }
        
        // 3. Return the next row from the iterator over filtered groups.
        if let Some(iter) = &mut self.output_rows_iter {
             Ok(iter.next())
        } else {
             // This case should ideally not be reachable if process_input_fully ran
             Ok(None) 
        }
    }

    fn close(&mut self) -> QueryResult<()> {
        let mut input_op = self.input.lock().unwrap();
        input_op.close()?;
        self.groups.clear();
        self.input_schema = None;
        self.processed_all_input = false;
        self.output_rows_iter = None;
        Ok(())
    }
}

impl HashAggregateOperator {
    pub fn new(
        input: Arc<Mutex<dyn Operator + Send + Sync>>,
        group_by_expressions_ast: Vec<Expression>,
        aggregate_select_expressions: Vec<PhysicalSelectExpression>,
        having_expression: Option<Expression>,
        output_select_list: Vec<PhysicalSelectExpression>,
    ) -> Self {
        let mut aggregates_for_computation = Vec::new();
        let mut seen_internal_keys = HashSet::new();

        // Parse aggregates needed for SELECT/output
        for select_expr in &output_select_list {
            if let ast::Expression::Aggregate { .. } = &select_expr.expression {
                match AggregateExpression::from_ast(&select_expr.expression) {
                    Ok(agg_expr) => {
                        if seen_internal_keys.insert(agg_expr.internal_lookup_key.clone()) {
                            aggregates_for_computation.push(agg_expr);
                        }
                    }
                    Err(e) => eprintln!("[WARN] Error parsing aggregate from select list: {:?}, err: {:?}", select_expr.expression, e),
                }
            }
        }
        // Parse aggregates needed for HAVING
        if let Some(ref having_expr_ast) = having_expression {
            // Need a helper to extract all aggregate AST nodes from an expression tree
            fn extract_aggregates(expr: &ast::Expression, found_aggs: &mut Vec<ast::Expression>) {
                 match expr {
                    ast::Expression::Aggregate { .. } => found_aggs.push(expr.clone()),
                    ast::Expression::BinaryOp { left, right, .. } => {
                        extract_aggregates(left, found_aggs);
                        extract_aggregates(right, found_aggs);
                    }
                    ast::Expression::UnaryOp { expr: inner_expr, .. } => extract_aggregates(inner_expr, found_aggs),
                    // Add other expression types if needed
                    _ => {},
                }
            }
            let mut having_aggregates_ast = Vec::new();
            extract_aggregates(having_expr_ast, &mut having_aggregates_ast);

            for agg_ast_node in having_aggregates_ast {
                 match AggregateExpression::from_ast(&agg_ast_node) {
                    Ok(agg_expr) => {
                        if seen_internal_keys.insert(agg_expr.internal_lookup_key.clone()) {
                            aggregates_for_computation.push(agg_expr);
                        }
                    }
                    Err(e) => eprintln!("[WARN] Error parsing aggregate from having expression: {:?}, err: {:?}", agg_ast_node, e),
                }
            }
        }
        
        HashAggregateOperator {
            input,
            group_by_expressions_ast,
            having_expression,
            output_select_list,
            aggregates_for_computation, // Store the computed list
            groups: HashMap::new(),
            input_schema: None,
            processed_all_input: false,
            output_rows_iter: None, 
        }
    }

    // New helper to fully process input and populate groups
    fn process_input_fully(&mut self) -> QueryResult<()> {
        if self.processed_all_input { return Ok(()); }

        let mut all_input_rows: Vec<Row> = Vec::new();
        let mut first_row_for_schema: Option<Row> = None;

        {
            let mut input_op_guard = self.input.lock().unwrap();

            // Read all rows first while holding the lock
            if self.input_schema.is_none() {
                if let Some(first_row) = input_op_guard.next()? {
                    first_row_for_schema = Some(first_row);
                } else {
                    // Input is empty, nothing to do
                    self.processed_all_input = true;
                    return Ok(());
                }
            }
            while let Some(row) = input_op_guard.next()? {
                all_input_rows.push(row);
            }
            // Lock `input_op_guard` is dropped here at the end of the scope
        }

        // Derive schema if needed, using the first row
        if let Some(first_row) = first_row_for_schema {
             // HACK for schema derivation
            let dummy_columns: Vec<Column> = first_row.columns().iter().map(|name| {
                let val_type = first_row.get(name)
                                    .map_or(CatalogDataType::Text, |dv| CatalogDataType::from_str(&dv.get_type().to_string()).unwrap_or(CatalogDataType::Text));
                Column::new(name.clone(), val_type, true, false, None)
            }).collect();
            self.input_schema = Some(Arc::new(Table::new("derived_input_schema".to_string(), dummy_columns)));

            // Process the first row now that the lock is released
            self.process_single_row(first_row)?;
        }

        // Process the rest of the rows
        for row in all_input_rows {
            self.process_single_row(row)?;
        }

        self.processed_all_input = true;
        Ok(())
    }

    // New helper to process a single row from input
    fn process_single_row(&mut self, row: Row) -> QueryResult<()> {
        let schema_ref = self.input_schema.as_ref()
            .ok_or_else(|| QueryError::ExecutionError("Input schema not available during row processing".to_string()))?; // Should be set by now

        // Evaluate group-by expressions for this row
        let current_group_key_values: Vec<DataValue> = self.group_by_expressions_ast.iter()
            .map(|expr| expression_eval::evaluate_expression(expr, &row, Some(schema_ref)))
            .collect::<Result<Vec<_>, _>>()?;

        // Find or create the group and update aggregates
        let group = self.groups.entry(current_group_key_values.clone()) // Clone key for insertion
            .or_insert_with(|| {
                 // Use the pre-computed list of aggregates
                Group::new(current_group_key_values, &self.aggregates_for_computation, self.group_by_expressions_ast.clone())
            });
        
        group.update(&row, schema_ref)?;
        Ok(())
    }

    // New helper function to transform aggregate expressions in HAVING
    fn transform_expression_for_having(
        original_expr: &Expression,
    ) -> QueryResult<Expression> {
        match original_expr {
            Expression::Aggregate { .. } => {
                // Use the internal representation (e.g., "COUNT(*)", "SUM(price)") as the column name
                // This assumes the row used for evaluation will have these keys.
                match AggregateExpression::from_ast(original_expr) {
                    Ok(agg_expr_parsed) => Ok(Expression::Column(ast::ColumnReference {
                        table: None, 
                        name: agg_expr_parsed.internal_lookup_key,
                    })),
                    Err(e) => Err(QueryError::ExecutionError(format!(
                        "Could not parse aggregate expression in HAVING clause for transformation: {:?}, error: {}",
                        original_expr, e
                    ))),
                }
            }
            Expression::BinaryOp { left, op, right } => {
                let new_left = Self::transform_expression_for_having(left)?;
                let new_right = Self::transform_expression_for_having(right)?;
                Ok(Expression::BinaryOp {
                    left: Box::new(new_left),
                    op: op.clone(),
                    right: Box::new(new_right),
                })
            }
            Expression::UnaryOp { op, expr } => {
                let new_expr = Self::transform_expression_for_having(expr)?;
                Ok(Expression::UnaryOp { op: op.clone(), expr: Box::new(new_expr) })
            }
            Expression::Literal(_) | Expression::Column(_) => Ok(original_expr.clone()),
            // TODO: Handle other expression types like Function, Case, IsNull if they can appear in HAVING
            // and potentially contain aggregates.
            _ => Err(QueryError::ExecutionError(format!(
                "Unsupported expression type {:?} in HAVING clause transformation",
                original_expr
            ))),
        }
    }
}

/// Create a new HashAggregateOperator (factory function)
pub fn create_hash_aggregate(
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    group_by_expressions: Vec<Expression>,
    aggregate_select_expressions: Vec<PhysicalSelectExpression>, // Used by HashAggregateOperator::new
    having_expression: Option<Expression>,
    output_select_list: Vec<PhysicalSelectExpression>, // Passed to HashAggregateOperator::new
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> { // Return QueryResult<Arc<Mutex<...>>>
    let operator = HashAggregateOperator::new(
        input, 
        group_by_expressions, 
        aggregate_select_expressions, 
        having_expression, 
        output_select_list
    );
    // Wrap in Arc<Mutex<>>
    Ok(Arc::new(Mutex::new(operator)))
}

/// Helper function to recursively extract all aggregate function expressions from a given Expression node.
fn extract_aggregates_from_expression(expression: &crate::query::parser::ast::Expression) -> Vec<crate::query::parser::ast::Expression> {
    let mut aggregates = Vec::new();
    match expression {
        crate::query::parser::ast::Expression::Aggregate { .. } => {
            aggregates.push(expression.clone());
        }
        crate::query::parser::ast::Expression::BinaryOp { left, right, .. } => {
            aggregates.extend(extract_aggregates_from_expression(left));
            aggregates.extend(extract_aggregates_from_expression(right));
        }
        crate::query::parser::ast::Expression::UnaryOp { expr, .. } => {
            aggregates.extend(extract_aggregates_from_expression(expr));
        }
        crate::query::parser::ast::Expression::Function { args, .. } => {
            for arg in args {
                aggregates.extend(extract_aggregates_from_expression(arg));
            }
        }
        crate::query::parser::ast::Expression::Case { operand, when_then_clauses, else_clause } => {
            if let Some(op_expr) = operand {
                aggregates.extend(extract_aggregates_from_expression(op_expr));
            }
            for (when_expr, then_expr) in when_then_clauses {
                aggregates.extend(extract_aggregates_from_expression(when_expr));
                aggregates.extend(extract_aggregates_from_expression(then_expr));
            }
            if let Some(else_expr) = else_clause {
                aggregates.extend(extract_aggregates_from_expression(else_expr));
            }
        }
        crate::query::parser::ast::Expression::IsNull { expr, .. } => {
            aggregates.extend(extract_aggregates_from_expression(expr));
        }
        crate::query::parser::ast::Expression::Literal(_) => {}
        crate::query::parser::ast::Expression::Column(_) => {}
    }
    aggregates
} 