// Hash Join Implementation
//
// This file implements the hash join algorithm, which is optimized for
// equality-based join conditions with O(n+m) time complexity.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{Row, QueryResult, QueryError, DataValue};

/// Hash Join operator implementation
pub struct HashJoin {
    /// Left input operator (build side)
    left: Arc<Mutex<dyn Operator + Send>>,
    /// Right input operator (probe side)
    right: Arc<Mutex<dyn Operator + Send>>,
    /// Join condition (must be an equality condition)
    condition: String,
    /// Left table alias
    left_alias: String,
    /// Right table alias
    right_alias: String,
    /// Hash table for join
    hash_table: HashMap<DataValue, Vec<Row>>,
    /// Indicates if the hash table has been built
    hash_table_built: bool,
    /// Current right row being processed
    current_right_row: Option<Row>,
    /// Current matches for the current right row
    current_matches: Vec<Row>,
    /// Current match index
    current_match_index: usize,
    /// Indicates if this is a LEFT OUTER JOIN
    is_left_join: bool,
    /// Rows from left side that didn't match (for LEFT JOIN)
    unmatched_left_rows: Vec<Row>,
    /// Current index in unmatched left rows
    unmatched_index: usize,
    /// Initialization status
    initialized: bool,
}

impl HashJoin {
    /// Create a new hash join operator
    pub fn new(
        left: Arc<Mutex<dyn Operator + Send>>,
        right: Arc<Mutex<dyn Operator + Send>>,
        condition: String,
        is_left_join: bool,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        HashJoin {
            left,
            right,
            condition,
            left_alias,
            right_alias,
            hash_table: HashMap::new(),
            hash_table_built: false,
            current_right_row: None,
            current_matches: Vec::new(),
            current_match_index: 0,
            is_left_join,
            unmatched_left_rows: Vec::new(),
            unmatched_index: 0,
            initialized: false,
        }
    }
    
    /// Parse the join condition to get the column names
    fn parse_condition(&self) -> Result<(String, String), QueryError> {
        // Parse the condition in the format "left.column = right.column"
        let parts: Vec<&str> = self.condition.split('=').collect();
        if parts.len() != 2 {
            return Err(QueryError::InvalidOperation(
                "Hash join requires an equality condition".to_string()
            ));
        }
        
        let left_col = parts[0].trim();
        let right_col = parts[1].trim();
        
        // For simplicity, we'll ignore table qualifiers
        let left_column = if left_col.contains('.') {
            let parts: Vec<&str> = left_col.split('.').collect();
            parts[1].to_string()
        } else {
            left_col.to_string()
        };
        
        let right_column = if right_col.contains('.') {
            let parts: Vec<&str> = right_col.split('.').collect();
            parts[1].to_string()
        } else {
            right_col.to_string()
        };
        
        Ok((left_column, right_column))
    }
    
    /// Build the hash table from the left relation
    fn build_hash_table(&mut self) -> QueryResult<()> {
        // Parse the join condition
        let (left_column, _) = self.parse_condition()?;
        
        // Build the hash table from the left relation
        loop {
            match self.left.lock().unwrap().next()? {
                Some(row) => {
                    // Get the join key
                    if let Some(key) = row.get(&left_column) {
                        // Add the row to the hash table
                        let entry = self.hash_table.entry(key.clone()).or_insert_with(Vec::new);
                        
                        // For LEFT JOIN, track all left rows
                        if self.is_left_join {
                            self.unmatched_left_rows.push(row.clone());
                        }
                        
                        entry.push(row);
                    }
                }
                None => break,
            }
        }
        
        self.hash_table_built = true;
        Ok(())
    }
    
    /// Find matches for the current right row
    fn find_matches(&mut self) -> QueryResult<()> {
        // Parse the join condition
        let (_, right_column) = self.parse_condition()?;
        
        // Clear previous matches
        self.current_matches.clear();
        self.current_match_index = 0;
        
        // Get the current right row
        if let Some(right_row) = &self.current_right_row {
            // Get the join key
            if let Some(key) = right_row.get(&right_column) {
                // Look up matches in the hash table
                if let Some(matches) = self.hash_table.get(key) {
                    // Mark these rows as matched (for LEFT JOIN)
                    if self.is_left_join {
                        for matched_row in matches {
                            // Remove matched rows from unmatched list
                            if let Some(pos) = self.unmatched_left_rows.iter().position(|r| r == matched_row) {
                                self.unmatched_left_rows.remove(pos);
                            }
                        }
                    }
                    
                    // Add all matches
                    self.current_matches = matches.clone();
                }
            }
        }
        
        Ok(())
    }
}

impl Operator for HashJoin {
    fn init(&mut self) -> QueryResult<()> {
        // Initialize both input operators
        self.left.lock().unwrap().init()?;
        self.right.lock().unwrap().init()?;
        
        // Build the hash table from the left relation
        if !self.hash_table_built {
            self.build_hash_table()?;
        }
        
        // Get the first right row
        self.current_right_row = self.right.lock().unwrap().next()?;
        
        // Find matches for the first right row
        if self.current_right_row.is_some() {
            self.find_matches()?;
        }
        
        self.initialized = true;
        Ok(())
    }
    
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if !self.initialized {
            self.init()?;
        }
        
        // Process any current matches
        if self.current_match_index < self.current_matches.len() {
            // Get the next match
            let left_row = &self.current_matches[self.current_match_index];
            self.current_match_index += 1;
            
            let mut joined_row = Row::new(); // Start with an empty row

            // Add columns from the left_row, prefixed with left_alias
            for left_col_name in left_row.columns() {
                if let Some(value) = left_row.get(left_col_name) {
                    // Only prefix if alias is not empty, to handle cases where root table might not have an alias
                    let qualified_name = if !self.left_alias.is_empty() {
                        format!("{}.{}", self.left_alias, left_col_name)
                    } else {
                        left_col_name.clone()
                    };
                    joined_row.set(qualified_name, value.clone());
                }
            }
            
            // Add all columns from right row, prefixed with right_alias
            if let Some(right_row) = &self.current_right_row {
                for right_col_name in right_row.columns() {
                    if let Some(value) = right_row.get(right_col_name) {
                        // Only prefix if alias is not empty
                        let qualified_name = if !self.right_alias.is_empty() {
                            format!("{}.{}", self.right_alias, right_col_name)
                        } else {
                            right_col_name.clone()
                        };
                        joined_row.set(qualified_name, value.clone());
                    }
                }
            }
            
            return Ok(Some(joined_row));
        }
        
        // Move to the next right row
        self.current_right_row = self.right.lock().unwrap().next()?;
        
        // If no more right rows, check if we need to output unmatched left rows (LEFT JOIN)
        if self.current_right_row.is_none() {
            if self.is_left_join && self.unmatched_index < self.unmatched_left_rows.len() {
                // Get the next unmatched left row
                let left_row = &self.unmatched_left_rows[self.unmatched_index];
                self.unmatched_index += 1;
                
                let mut joined_row = Row::new(); // Start with an empty row
                for left_col_name in left_row.columns() {
                    if let Some(value) = left_row.get(left_col_name) {
                        // Only prefix if alias is not empty
                        let qualified_name = if !self.left_alias.is_empty() {
                            format!("{}.{}", self.left_alias, left_col_name)
                        } else {
                            left_col_name.clone()
                        };
                        joined_row.set(qualified_name, value.clone());
                    }
                }
                
                // TODO: Add NULL values for all right columns, prefixed with right_alias (if not empty).
                // This requires knowing the schema of the right input.
                // For example, if right schema has columns c1, c2:
                // if !self.right_alias.is_empty() {
                //     joined_row.set(format!("{}.c1", self.right_alias), DataValue::Null);
                //     joined_row.set(format!("{}.c2", self.right_alias), DataValue::Null);
                // } else {
                //     joined_row.set("c1".to_string(), DataValue::Null);
                //     joined_row.set("c2".to_string(), DataValue::Null);
                // }

                return Ok(Some(joined_row));
            }
            
            // No more rows to process
            return Ok(None);
        }
        
        // Find matches for the new right row
        self.find_matches()?;
        
        // Recurse to process the matches
        self.next()
    }
    
    fn close(&mut self) -> QueryResult<()> {
        // Close both input operators
        self.left.lock().unwrap().close()?;
        self.right.lock().unwrap().close()?;
        self.initialized = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::query::executor::operators::join::tests::{MockOperator, create_test_row, create_order_row};

    #[test]
    fn test_hash_join() {
        // Create test data
        let left_rows = vec![
            create_test_row(1, "Alice"),
            create_test_row(2, "Bob"),
            create_test_row(3, "Charlie"),
        ];
        
        let right_rows = vec![
            create_order_row(1, 101),
            create_order_row(2, 102),
        ];
        
        // Create operators
        let left_op = Arc::new(Mutex::new(MockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(MockOperator::new(right_rows)));
        
        // Create hash join operator
        let mut join_op = HashJoin::new(
            left_op.clone(),
            right_op.clone(),
            "id = id".to_string(),
            false,
            "left_table".to_string(),
            "right_table".to_string(),
        );
        
        // Initialize the operator
        join_op.init().unwrap();
        
        // Get joined rows
        let row1 = join_op.next().unwrap().unwrap();
        assert_eq!(row1.get("left_table.id"), Some(&DataValue::Integer(1)));
        assert_eq!(row1.get("left_table.name"), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(row1.get("right_table.id"), Some(&DataValue::Integer(1)));
        assert_eq!(row1.get("right_table.order_id"), Some(&DataValue::Integer(101)));
        
        let row2 = join_op.next().unwrap().unwrap();
        assert_eq!(row2.get("left_table.id"), Some(&DataValue::Integer(2)));
        assert_eq!(row2.get("left_table.name"), Some(&DataValue::Text("Bob".to_string())));
        assert_eq!(row2.get("right_table.id"), Some(&DataValue::Integer(2)));
        assert_eq!(row2.get("right_table.order_id"), Some(&DataValue::Integer(102)));
        
        // No more joined rows
        assert!(join_op.next().unwrap().is_none());
    }
    
    #[test]
    fn test_hash_join_left_join() {
        // Create test data for LEFT JOIN scenario
        let left_rows = vec![
            create_test_row(1, "Alice"),
            create_test_row(2, "Bob"),
            create_test_row(3, "Charlie"), // No matching right row
        ];
        
        let right_rows = vec![
            create_order_row(1, 101),
            create_order_row(2, 102),
        ];
        
        // Create operators
        let left_op = Arc::new(Mutex::new(MockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(MockOperator::new(right_rows)));
        
        // Create hash join operator with LEFT JOIN
        let mut join_op = HashJoin::new(
            left_op.clone(),
            right_op.clone(),
            "id = id".to_string(),
            true, // is_left_join = true
            "left_table".to_string(),
            "right_table".to_string(),
        );
        
        // Initialize the operator
        join_op.init().unwrap();
        
        // Collect all rows to check after
        let mut results = Vec::new();
        while let Some(row) = join_op.next().unwrap() {
            results.push(row);
        }
        
        // Should have 3 rows (including unmatched Charlie)
        assert_eq!(results.len(), 3);
        
        // Verify all users are present
        let names: Vec<String> = results.iter()
            .filter_map(|r| r.get("left_table.name"))
            .filter_map(|v| match v {
                DataValue::Text(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));
        assert!(names.contains(&"Charlie".to_string()));
        
        // Verify matched users have order_id, and unmatched have None for right table columns
        for row in &results {
            if let Some(DataValue::Text(name_val)) = row.get("left_table.name") {
                if name_val == "Alice" || name_val == "Bob" {
                    assert!(row.get("right_table.order_id").is_some());
                    assert!(row.get("right_table.id").is_some());
                } else if name_val == "Charlie" {
                    assert_eq!(row.get("right_table.id"), None, "right_table.id should be None for unmatched Charlie");
                    assert_eq!(row.get("right_table.order_id"), None, "right_table.order_id should be None for unmatched Charlie");
                }
            }
        }
    }
} 