// Sort-based Aggregation Operator
//
// This operator implements aggregation by sorting rows by the group-by keys
// and then computing aggregates for each group sequentially
//
// This implementation is useful for large datasets where hash-based aggregation
// would consume too much memory.

use std::sync::{Arc, Mutex};
use std::cmp::Ordering;
use std::collections::HashMap;
use crate::query::executor::result::{Row, QueryResult, QueryError, DataValue};
use crate::query::executor::operators::Operator;
use super::AggregateType;

/// Helper for parsing aggregate expressions
#[derive(Debug, Clone)]
struct AggregateExpression {
    /// Type of aggregation (COUNT, SUM, etc.)
    agg_type: AggregateType,
    /// Column to aggregate
    column: Option<String>,
    /// Output column name
    output_name: String,
}

impl AggregateExpression {
    /// Parse an aggregate expression string into structured form
    /// Format: "COUNT(column)" or "SUM(column)" or "AVG(column)"
    fn parse(expr_str: &str) -> QueryResult<Self> {
        let expr = expr_str.trim();
        
        // Handle COUNT(*) special case
        if expr.to_uppercase() == "COUNT(*)" {
            return Ok(AggregateExpression {
                agg_type: AggregateType::Count,
                column: None,
                output_name: "COUNT(*)".to_string(),
            });
        }

        // Parse function name and argument
        let open_paren = expr.find('(').ok_or_else(|| 
            QueryError::ExecutionError(format!("Invalid aggregate expression: {}", expr))
        )?;
        
        let close_paren = expr.rfind(')').ok_or_else(|| 
            QueryError::ExecutionError(format!("Invalid aggregate expression: {}", expr))
        )?;

        let function_name = expr[..open_paren].trim().to_uppercase();
        let column_name = expr[open_paren+1..close_paren].trim().to_string();

        // Convert function name to AggregateType
        let agg_type = match function_name.as_str() {
            "COUNT" => AggregateType::Count,
            "SUM" => AggregateType::Sum,
            "AVG" => AggregateType::Avg,
            "MIN" => AggregateType::Min,
            "MAX" => AggregateType::Max,
            _ => return Err(QueryError::ExecutionError(
                format!("Unsupported aggregate function: {}", function_name)
            )),
        };

        Ok(AggregateExpression {
            agg_type,
            column: Some(column_name),
            output_name: expr.to_string(),
        })
    }
}

/// SortAggregateOperator performs grouping and aggregation using sorting
/// 
/// This operator first sorts the input by the group-by keys, then processes
/// the data in a streaming fashion, computing aggregates for each group.
pub struct SortAggregateOperator {
    // Input operator
    input: Arc<Mutex<dyn Operator>>,
    // Group by column references
    group_by_columns: Vec<String>,
    // Aggregate expressions
    aggregate_expressions: Vec<String>,
    // Having clause (optional)
    having: Option<String>,
    // Has this operator been initialized
    initialized: bool,
    // Current group being processed
    current_group_key: Option<Vec<DataValue>>,
    // Current group's aggregates
    current_aggregates: HashMap<String, DataValue>,
    // Current row to return (used during iteration)
    current_row: Option<Row>,
    // Parsed aggregate expressions
    parsed_agg_expressions: Vec<AggregateExpression>,
    // Whether all input has been processed
    input_exhausted: bool,
}

impl Operator for SortAggregateOperator {
    fn init(&mut self) -> QueryResult<()> {
        if self.initialized {
            return Ok(());
        }
        
        // Initialize the input operator
        let mut input = self.input.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
        })?;
        input.init()?;
        
        // Parse aggregate expressions
        for expr_str in &self.aggregate_expressions {
            let agg_expr = AggregateExpression::parse(expr_str)?;
            self.parsed_agg_expressions.push(agg_expr);
        }
        
        self.initialized = true;
        self.input_exhausted = false;
        
        Ok(())
    }
    
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if !self.initialized {
            self.init()?;
        }
        
        // If we have a row ready to return, return it
        if let Some(row) = self.current_row.take() {
            return Ok(Some(row));
        }
        
        // If we're done processing, return None
        if self.input_exhausted {
            return Ok(None);
        }
        
        // Process input until we have a complete group
        self.process_next_group()?;
        
        // Return the current row if we have one
        Ok(self.current_row.take())
    }
    
    fn close(&mut self) -> QueryResult<()> {
        let mut input = self.input.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
        })?;
        input.close()?;
        
        self.initialized = false;
        self.current_group_key = None;
        self.current_aggregates.clear();
        self.current_row = None;
        self.input_exhausted = true;
        
        Ok(())
    }
}

impl SortAggregateOperator {
    /// Create a new SortAggregateOperator
    pub fn new(
        input: Arc<Mutex<dyn Operator>>,
        group_by_columns: Vec<String>,
        aggregate_expressions: Vec<String>,
        having: Option<String>
    ) -> Self {
        SortAggregateOperator {
            input,
            group_by_columns,
            aggregate_expressions,
            having,
            initialized: false,
            current_group_key: None,
            current_aggregates: HashMap::new(),
            current_row: None,
            parsed_agg_expressions: Vec::new(),
            input_exhausted: false,
        }
    }
    
    /// Process the next group from the input
    fn process_next_group(&mut self) -> QueryResult<()> {
        let mut input = self.input.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
        })?;
        
        // Get the first row to start a group
        let first_row = if let Some(row) = input.next()? {
            row
        } else {
            // No more input rows
            self.input_exhausted = true;
            return Ok(());
        };
        
        // Extract the group key from the first row
        let group_key = self.extract_group_key(&first_row)?;
        
        // Initialize aggregates for this group
        let mut aggregates: HashMap<String, AggregateValue> = HashMap::new();
        for agg_expr in &self.parsed_agg_expressions {
            let agg_value = AggregateValue::new(agg_expr.agg_type.clone());
            aggregates.insert(agg_expr.output_name.clone(), agg_value);
        }
        
        // Update aggregates with the first row
        self.update_aggregates(&mut aggregates, &first_row)?;
        
        // Process rows until we find a row with a different group key
        loop {
            let next_row = input.next()?;
            
            if let Some(row) = next_row {
                let next_key = self.extract_group_key(&row)?;
                
                if self.keys_equal(&group_key, &next_key) {
                    // Same group, update aggregates
                    self.update_aggregates(&mut aggregates, &row)?;
                } else {
                    // New group found, finalize current group
                    let result_row = self.finalize_group(&group_key, &aggregates)?;
                    
                    // Store the new row so we can process it in the next call
                    drop(input); // Release lock before storing state
                    self.current_group_key = Some(next_key);
                    self.current_row = Some(result_row);
                    return Ok(());
                }
            } else {
                // No more rows, finalize current group
                let result_row = self.finalize_group(&group_key, &aggregates)?;
                self.input_exhausted = true;
                self.current_row = Some(result_row);
                return Ok(());
            }
        }
    }
    
    /// Extract the group key from a row
    fn extract_group_key(&self, row: &Row) -> QueryResult<Vec<DataValue>> {
        let mut key = Vec::with_capacity(self.group_by_columns.len());
        
        for col in &self.group_by_columns {
            let value = match row.get(col) {
                Some(val) => val.clone(),
                None => return Err(QueryError::ColumnNotFound(col.clone())),
            };
            key.push(value);
        }
        
        Ok(key)
    }
    
    /// Check if two group keys are equal
    fn keys_equal(&self, key1: &[DataValue], key2: &[DataValue]) -> bool {
        if key1.len() != key2.len() {
            return false;
        }
        
        for (v1, v2) in key1.iter().zip(key2.iter()) {
            match v1.partial_cmp(v2) {
                Some(Ordering::Equal) => continue,
                _ => return false,
            }
        }
        
        true
    }
    
    /// Update aggregates with a row
    fn update_aggregates(&self, aggregates: &mut HashMap<String, AggregateValue>, row: &Row) -> QueryResult<()> {
        for agg_expr in &self.parsed_agg_expressions {
            let agg_value = aggregates.get_mut(&agg_expr.output_name).unwrap();
            
            // Get value to aggregate (if any)
            let value = if let Some(col) = &agg_expr.column {
                row.get(col)
            } else {
                None // For COUNT(*)
            };
            
            // Update the aggregate value
            agg_value.update(value);
        }
        
        Ok(())
    }
    
    /// Finalize a group and create a result row
    fn finalize_group(&self, group_key: &[DataValue], aggregates: &HashMap<String, AggregateValue>) -> QueryResult<Row> {
        let mut row = Row::new();
        
        // Add group by columns
        for (i, col) in self.group_by_columns.iter().enumerate() {
            row.set(col.clone(), group_key[i].clone());
        }
        
        // Add aggregate values
        for agg_expr in &self.parsed_agg_expressions {
            let agg_value = aggregates.get(&agg_expr.output_name).unwrap();
            row.set(agg_expr.output_name.clone(), agg_value.get_result());
        }
        
        // Apply HAVING filter if present
        if let Some(having) = &self.having {
            // TODO: Implement HAVING filter evaluation
            // For now, just pass through all groups
            println!("HAVING filter not yet applied: {}", having);
        }
        
        Ok(row)
    }
}

/// Create a new SortAggregateOperator
pub fn create_sort_aggregate(
    input: Arc<Mutex<dyn Operator>>,
    group_by_columns: Vec<String>,
    aggregate_expressions: Vec<String>,
    having: Option<String>
) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    let operator = SortAggregateOperator::new(
        input,
        group_by_columns,
        aggregate_expressions,
        having
    );
    
    Ok(Arc::new(Mutex::new(operator)))
}

/// Helper struct for aggregate calculations
#[derive(Debug, Clone)]
struct AggregateValue {
    /// Type of aggregation
    agg_type: AggregateType,
    /// Count of records
    count: i64,
    /// Sum value for SUM/AVG
    sum: Option<DataValue>,
    /// Min value for MIN
    min: Option<DataValue>,
    /// Max value for MAX
    max: Option<DataValue>,
}

impl AggregateValue {
    /// Create a new aggregate value
    fn new(agg_type: AggregateType) -> Self {
        AggregateValue {
            agg_type,
            count: 0,
            sum: None,
            min: None,
            max: None,
        }
    }

    /// Update the aggregate with a new value
    fn update(&mut self, value: Option<&DataValue>) {
        // Increment count for all aggregate types
        self.count += 1;

        // If value is None, only update count - handle NULL values gracefully
        let Some(value) = value else {
            return;
        };

        // Update aggregate based on type
        match self.agg_type {
            AggregateType::Count => {
                // Count is already updated
            },
            AggregateType::Sum | AggregateType::Avg => {
                // Update sum for both SUM and AVG
                self.update_sum(value);
            },
            AggregateType::Min => {
                // Update minimum value
                self.update_min(value);
            },
            AggregateType::Max => {
                // Update maximum value
                self.update_max(value);
            },
        }
    }

    /// Update sum value
    fn update_sum(&mut self, value: &DataValue) {
        match value {
            DataValue::Integer(i) => {
                match &mut self.sum {
                    None => self.sum = Some(DataValue::Integer(*i)),
                    Some(DataValue::Integer(sum)) => *sum += *i,
                    Some(DataValue::Float(sum)) => *sum += *i as f64,
                    _ => {} // Ignore incompatible types
                }
            },
            DataValue::Float(f) => {
                match &mut self.sum {
                    None => self.sum = Some(DataValue::Float(*f)),
                    Some(DataValue::Integer(sum)) => {
                        // Convert integer sum to float when adding float value
                        self.sum = Some(DataValue::Float(*sum as f64 + *f));
                    },
                    Some(DataValue::Float(sum)) => *sum += *f,
                    _ => {} // Ignore incompatible types
                }
            },
            _ => {} // Ignore other types
        }
    }

    /// Update min value
    fn update_min(&mut self, value: &DataValue) {
        match &self.min {
            None => self.min = Some(value.clone()),
            Some(current_min) => {
                if let Some(Ordering::Greater) = current_min.partial_cmp(value) {
                    self.min = Some(value.clone());
                }
            }
        }
    }

    /// Update max value
    fn update_max(&mut self, value: &DataValue) {
        match &self.max {
            None => self.max = Some(value.clone()),
            Some(current_max) => {
                if let Some(Ordering::Less) = current_max.partial_cmp(value) {
                    self.max = Some(value.clone());
                }
            }
        }
    }

    /// Get the final aggregate value
    fn get_result(&self) -> DataValue {
        match self.agg_type {
            AggregateType::Count => {
                DataValue::Integer(self.count)
            },
            AggregateType::Sum => {
                self.sum.clone().unwrap_or(DataValue::Null)
            },
            AggregateType::Avg => {
                match self.sum.clone() {
                    Some(DataValue::Integer(sum)) => {
                        if self.count > 0 {
                            DataValue::Float(sum as f64 / self.count as f64)
                        } else {
                            DataValue::Null
                        }
                    },
                    Some(DataValue::Float(sum)) => {
                        if self.count > 0 {
                            DataValue::Float(sum / self.count as f64)
                        } else {
                            DataValue::Null
                        }
                    },
                    _ => DataValue::Null
                }
            },
            AggregateType::Min => {
                self.min.clone().unwrap_or(DataValue::Null)
            },
            AggregateType::Max => {
                self.max.clone().unwrap_or(DataValue::Null)
            },
        }
    }
} 