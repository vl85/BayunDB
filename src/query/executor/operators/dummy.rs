// Dummy operator module - can be used for testing

use crate::query::executor::result::{Row, QueryResult};
use super::Operator;

#[derive(Debug, Default)]
pub struct DummyOperator;

impl DummyOperator {
    pub fn new() -> Self {
        DummyOperator
    }
}

impl Operator for DummyOperator {
    fn init(&mut self) -> QueryResult<()> {
        Ok(())
    }

    fn next(&mut self) -> QueryResult<Option<Row>> {
        // Dummy operator produces no rows
        Ok(None)
    }

    fn close(&mut self) -> QueryResult<()> {
        Ok(())
    }
} 