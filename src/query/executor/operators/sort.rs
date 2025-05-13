use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{QueryResult, Row};
use crate::query::parser::ast::Expression; 
// use crate::catalog::Table; // May need for expression evaluation context eventually
use crate::query::executor::expression_eval;

pub struct SortOperator {
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    // Each tuple: (expression_to_extract_sort_key, is_descending)
    order_by: Vec<(Expression, bool)>, 
    // For evaluating expressions, we might need the schema of the input rows.
    // This would be set during init based on the input operator's output.
    // For now, if expressions are simple column names, Row::get() might suffice.
    // input_schema: Option<Table>, // Placeholder for future expression evaluation context
    
    // sorted_rows: Option<Vec<Row>>, // Removed - logic moved to next()
    output_iter: Option<std::vec::IntoIter<Row>>,
}

impl SortOperator {
    pub fn new(input: Arc<Mutex<dyn Operator + Send + Sync>>, order_by: Vec<(Expression, bool)>) -> Self {
        SortOperator {
            input,
            order_by,
            // input_schema: None,
            // sorted_rows: None, // Removed
            output_iter: None,
        }
    }
}

impl Operator for SortOperator {
    fn init(&mut self) -> QueryResult<()> {
        // Reset state for potential re-use, input init happens in next()
        // self.sorted_rows = None; // Removed
        self.output_iter = None;
        Ok(())
    }

    fn next(&mut self) -> QueryResult<Option<Row>> {
        // If the output iterator hasn't been created yet, sort the input and create it.
        if self.output_iter.is_none() {
            // Read all input rows and close input operator.
            let mut all_rows = Vec::new();
            {
                // Scope for input lock
                let mut input_op = self.input.lock().unwrap();
                input_op.init()?; // Initialize input
                while let Some(row) = input_op.next()? {
                    all_rows.push(row);
                }
                input_op.close()?; // Close input immediately after reading
            } // Input lock released

            // Sort the collected rows
            all_rows.sort_by(|a, b| {
                for (expr, is_desc) in &self.order_by {
                    // HACK: Evaluate expressions without schema. Needs schema handling.
                    let val_a = expression_eval::evaluate_expression(expr, a, None);
                    let val_b = expression_eval::evaluate_expression(expr, b, None);

                    match (val_a, val_b) {
                        (Ok(va), Ok(vb)) => {
                            match va.compare(&vb) { // Use DataValue::compare for NULL handling
                                Ok(Ordering::Equal) => continue, // Try next sort key
                                Ok(ord) => {
                                    return if *is_desc { ord.reverse() } else { ord }; // Apply DESC if needed
                                }
                                Err(e) => {
                                    // Handle comparison error - treat as equal for stability?
                                    eprintln!("Comparison error during sort: {:?}. Treating as Equal.", e);
                                    return Ordering::Equal;
                                }
                            }
                        }
                        // Handle evaluation errors - treat as equal for stability?
                        (Err(e), _) | (_, Err(e)) => {
                            eprintln!("Evaluation error during sort: {:?}. Treating as Equal.", e);
                            return Ordering::Equal;
                        }
                    }
                }
                Ordering::Equal // Rows are equal according to all keys
            });

            // Store the iterator over the sorted rows
            self.output_iter = Some(all_rows.into_iter());
        }

        // Now, consume from the iterator.
        // .as_mut() gets a mutable reference to the Option's content.
        // .unwrap() is safe because we ensured output_iter is Some above.
        // .next() consumes the next item from the iterator.
        Ok(self.output_iter.as_mut().unwrap().next())
    }

    fn close(&mut self) -> QueryResult<()> {
        // self.sorted_rows = None; // Removed
        self.output_iter = None;
        // Input operator was already closed within the next() method upon first call.
        // Attempting to close it again here would likely require re-locking and might error
        // if the input was already closed. Better to ensure it's closed after reading in next().
        Ok(())
    }
}

pub fn create_sort_operator(
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    order_by: Vec<(Expression, bool)>,
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    let op = SortOperator::new(input, order_by);
    Ok(Arc::new(Mutex::new(op)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::result::{DataValue, QueryError, QueryResult, Row};
    use crate::query::parser::ast::{Expression, ColumnReference, Value}; // Removed AstOperator import
    use std::sync::{Arc, Mutex};

    // MockOperator for providing controlled input to SortOperator
    struct MockOperator {
        rows: Vec<Row>,
        cursor: usize,
        initialized: bool,
    }

    impl MockOperator {
        fn new(rows: Vec<Row>) -> Self {
            MockOperator {
                rows,
                cursor: 0,
                initialized: false,
            }
        }
    }

    impl Operator for MockOperator {
        fn init(&mut self) -> QueryResult<()> {
            self.cursor = 0;
            self.initialized = true;
            Ok(())
        }

        fn next(&mut self) -> QueryResult<Option<Row>> {
            if !self.initialized {
                return Err(QueryError::ExecutionError("MockOperator not initialized".to_string()));
            }
            if self.cursor < self.rows.len() {
                let row = self.rows[self.cursor].clone();
                self.cursor += 1;
                Ok(Some(row))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> QueryResult<()> {
            self.initialized = false;
            Ok(())
        }
    }

    fn create_row(values: Vec<(&str, DataValue)>) -> Row {
        let mut row = Row::new();
        for (name, val) in values {
            row.set(name.to_string(), val);
        }
        row
    }

    #[test]
    fn test_empty_input() {
        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![])));
        let order_by_expr = Expression::Column(ColumnReference { table: None, name: "id".to_string() });
        let order_by = vec![(order_by_expr, false)]; // Sort by "id" ASC
        
        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();
        
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_single_row_input() {
        let row1 = create_row(vec![("id", DataValue::Integer(1))]);
        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row1.clone()])));
        let order_by_expr = Expression::Column(ColumnReference { table: None, name: "id".to_string() });
        let order_by = vec![(order_by_expr, false)];
        
        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();
        
        let sorted_row = sort_op.next().unwrap().unwrap();
        assert_eq!(sorted_row.get("id"), Some(&DataValue::Integer(1)));
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_simple_integer_sort_asc() {
        let row1 = create_row(vec![("id", DataValue::Integer(3)), ("name", DataValue::Text("C".to_string()))]);
        let row2 = create_row(vec![("id", DataValue::Integer(1)), ("name", DataValue::Text("A".to_string()))]);
        let row3 = create_row(vec![("id", DataValue::Integer(2)), ("name", DataValue::Text("B".to_string()))]);
        
        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row1, row2, row3])));
        let order_by_expr = Expression::Column(ColumnReference { table: None, name: "id".to_string() });
        let order_by = vec![(order_by_expr, false)]; // Sort by "id" ASC
        
        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();
        
        let r1 = sort_op.next().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&DataValue::Integer(1)));
        let r2 = sort_op.next().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&DataValue::Integer(2)));
        let r3 = sort_op.next().unwrap().unwrap();
        assert_eq!(r3.get("id"), Some(&DataValue::Integer(3)));
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_simple_integer_sort_desc() {
        let row1 = create_row(vec![("id", DataValue::Integer(3)), ("name", DataValue::Text("C".to_string()))]);
        let row2 = create_row(vec![("id", DataValue::Integer(1)), ("name", DataValue::Text("A".to_string()))]);
        let row3 = create_row(vec![("id", DataValue::Integer(2)), ("name", DataValue::Text("B".to_string()))]);
        
        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row1, row2, row3])));
        let order_by_expr = Expression::Column(ColumnReference { table: None, name: "id".to_string() });
        let order_by = vec![(order_by_expr, true)]; // Sort by "id" DESC
        
        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();
        
        let r1 = sort_op.next().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&DataValue::Integer(3)));
        let r2 = sort_op.next().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&DataValue::Integer(2)));
        let r3 = sort_op.next().unwrap().unwrap();
        assert_eq!(r3.get("id"), Some(&DataValue::Integer(1)));
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_string_sort_asc() {
        let row1 = create_row(vec![("name", DataValue::Text("Charlie".to_string()))]);
        let row2 = create_row(vec![("name", DataValue::Text("Alice".to_string()))]);
        let row3 = create_row(vec![("name", DataValue::Text("Bob".to_string()))]);
        
        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row1, row2, row3])));
        let order_by_expr = Expression::Column(ColumnReference { table: None, name: "name".to_string() });
        let order_by = vec![(order_by_expr, false)]; // Sort by "name" ASC
        
        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();
        
        let r1 = sort_op.next().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&DataValue::Text("Alice".to_string())));
        let r2 = sort_op.next().unwrap().unwrap();
        assert_eq!(r2.get("name"), Some(&DataValue::Text("Bob".to_string())));
        let r3 = sort_op.next().unwrap().unwrap();
        assert_eq!(r3.get("name"), Some(&DataValue::Text("Charlie".to_string())));
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_sort_nulls_first_asc() {
        // DataValue::partial_cmp for Nulls: Null is considered less than any other value.
        let row1 = create_row(vec![("id", DataValue::Integer(2))]);
        let row2 = create_row(vec![("id", DataValue::Null)]);
        let row3 = create_row(vec![("id", DataValue::Integer(1))]);

        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row1, row2, row3])));
        let order_by_expr = Expression::Column(ColumnReference { table: None, name: "id".to_string() });
        let order_by = vec![(order_by_expr, false)]; // ASC

        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();

        let r_null = sort_op.next().unwrap().unwrap();
        assert_eq!(r_null.get("id"), Some(&DataValue::Null));
        let r_one = sort_op.next().unwrap().unwrap();
        assert_eq!(r_one.get("id"), Some(&DataValue::Integer(1)));
        let r_two = sort_op.next().unwrap().unwrap();
        assert_eq!(r_two.get("id"), Some(&DataValue::Integer(2)));
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_sort_nulls_last_desc() {
        // DataValue::partial_cmp for Nulls: Null is considered less than any other value.
        // So for DESC, Nulls should effectively come last.
        let row1 = create_row(vec![("id", DataValue::Integer(2))]);
        let row2 = create_row(vec![("id", DataValue::Null)]);
        let row3 = create_row(vec![("id", DataValue::Integer(1))]);

        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row1, row2, row3])));
        let order_by_expr = Expression::Column(ColumnReference { table: None, name: "id".to_string() });
        let order_by = vec![(order_by_expr, true)]; // DESC

        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();

        let r_two = sort_op.next().unwrap().unwrap();
        assert_eq!(r_two.get("id"), Some(&DataValue::Integer(2)));
        let r_one = sort_op.next().unwrap().unwrap();
        assert_eq!(r_one.get("id"), Some(&DataValue::Integer(1)));
        let r_null = sort_op.next().unwrap().unwrap();
        assert_eq!(r_null.get("id"), Some(&DataValue::Null));
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_multi_column_sort() {
        let row1 = create_row(vec![("group", DataValue::Integer(1)), ("name", DataValue::Text("B".to_string()))]);
        let row2 = create_row(vec![("group", DataValue::Integer(2)), ("name", DataValue::Text("A".to_string()))]);
        let row3 = create_row(vec![("group", DataValue::Integer(1)), ("name", DataValue::Text("A".to_string()))]);

        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row1, row2, row3])));        
        let order_by = vec![
            (Expression::Column(ColumnReference { table: None, name: "group".to_string() }), false), // group ASC
            (Expression::Column(ColumnReference { table: None, name: "name".to_string() }), false)  // name ASC
        ];
        
        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();

        // Expected order: (1, A), (1, B), (2, A)
        let r_1a = sort_op.next().unwrap().unwrap();
        assert_eq!(r_1a.get("group"), Some(&DataValue::Integer(1)));
        assert_eq!(r_1a.get("name"), Some(&DataValue::Text("A".to_string())));

        let r_1b = sort_op.next().unwrap().unwrap();
        assert_eq!(r_1b.get("group"), Some(&DataValue::Integer(1)));
        assert_eq!(r_1b.get("name"), Some(&DataValue::Text("B".to_string())));

        let r_2a = sort_op.next().unwrap().unwrap();
        assert_eq!(r_2a.get("group"), Some(&DataValue::Integer(2)));
        assert_eq!(r_2a.get("name"), Some(&DataValue::Text("A".to_string())));
        
        assert!(sort_op.next().unwrap().is_none());
        sort_op.close().unwrap();
    }

    #[test]
    fn test_sort_by_case_expression() {
        // ORDER BY CASE WHEN val IS NULL THEN 0 ELSE 1 END, val ASC
        // This means NULLs come first (CASE yields 0), then non-NULLs (CASE yields 1), 
        // and non-NULLs are sorted by `val`.
        let row_null = create_row(vec![("val", DataValue::Null)]);
        let row_123 = create_row(vec![("val", DataValue::Integer(123))]);
        let row_neg_456 = create_row(vec![("val", DataValue::Integer(-456))]);
        let row_0 = create_row(vec![("val", DataValue::Integer(0))]);

        let input_op = Arc::new(Mutex::new(MockOperator::new(vec![row_123.clone(), row_null.clone(), row_0.clone(), row_neg_456.clone()]))); 

        let case_expr = Expression::Case {
            operand: None,
            when_then_clauses: vec![(
                Box::new(Expression::IsNull { // WHEN val IS NULL
                    expr: Box::new(Expression::Column(ColumnReference { table: None, name: "val".to_string() })),
                    not: false,
                }),
                Box::new(Expression::Literal(Value::Integer(0))) // THEN 0
            )],
            else_clause: Some(Box::new(Expression::Literal(Value::Integer(1)))), // ELSE 1
        };

        let order_by_col_val = Expression::Column(ColumnReference { table: None, name: "val".to_string() });

        let order_by = vec![
            (case_expr, false),         // CASE expr ASC
            (order_by_col_val, false) // val ASC
        ];

        let mut sort_op = SortOperator::new(input_op, order_by);
        sort_op.init().unwrap();

        // Expected order: NULL, -456, 0, 123
        let r_n = sort_op.next().unwrap().unwrap();
        assert_eq!(r_n.get("val"), Some(&DataValue::Null), "Expected Null first");

        let r_neg = sort_op.next().unwrap().unwrap();
        assert_eq!(r_neg.get("val"), Some(&DataValue::Integer(-456)), "Expected -456 second");

        let r_z = sort_op.next().unwrap().unwrap();
        assert_eq!(r_z.get("val"), Some(&DataValue::Integer(0)), "Expected 0 third");

        let r_pos = sort_op.next().unwrap().unwrap();
        assert_eq!(r_pos.get("val"), Some(&DataValue::Integer(123)), "Expected 123 fourth");

        assert!(sort_op.next().unwrap().is_none(), "Expected no more rows");
        sort_op.close().unwrap();
    }
} 