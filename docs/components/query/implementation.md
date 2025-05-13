# Query Parser/Executor Implementation TODO List

## Architecture

The query processing system will follow this architecture:

```
src/query/
├── parser/         # SQL parsing 
│   ├── lexer.rs    # Tokenizes SQL input
│   ├── ast.rs      # Abstract Syntax Tree nodes
│   └── parser.rs   # Builds AST from tokens
├── planner/        # Query planning
│   ├── logical.rs  # Logical query plan
│   ├── physical.rs # Physical execution plan  
│   └── optimizer.rs # Query optimization
├── executor/       # Query execution
│   ├── engine.rs   # Execution engine
│   ├── operators/  # Query operators
│   │   ├── scan.rs # Table/Index scan
│   │   ├── filter.rs # Filtering
│   │   ├── join.rs # Join operations
│   │   └── agg.rs  # Aggregation
│   └── result.rs   # Query result handling
└── mod.rs          # Module definitions
```

> **Note:** As of July 2024, JOIN operations (NestedLoopJoin, HashJoin), aggregation (GROUP BY, HAVING, aggregate functions), and predicate pushdown are fully implemented and tested. See the [roadmap](../../project/roadmap.md) for current and future work.

## Phase 1: Basic Query Parsing

- [x] Define token types for SQL lexing
- [x] Implement lexer to tokenize SQL strings
- [x] Define AST node structures for basic queries
  - [x] SELECT statement
  - [x] Column references
  - [x] Table references
  - [x] WHERE predicates
  - [x] Basic expressions (literals, operators)
- [x] Implement recursive descent parser
  - [x] Parse SELECT clause
  - [x] Parse FROM clause
  - [x] Parse WHERE clause
  - [x] Handle basic expressions
- [x] Add error reporting for syntax issues
- [x] Create integration tests for parser

## Phase 2: Logical Planning

- [x] Define logical operator interfaces
  - [x] LogicalScan
  - [x] LogicalFilter
  - [x] LogicalProjection
- [x] Implement logical plan builder from AST
- [x] Add type checking and semantic analysis
  - [x] Column reference resolution
  - [x] Type compatibility in expressions
  - [x] Table existence verification
- [x] Implement WHERE clause predicate analysis
- [x] Add logical plan visualization for debugging
- [x] Create tests for logical planning

## Phase 3: Physical Planning

- [x] Define physical operator interfaces
- [x] Implement basic physical operators
  - [x] TableScan
  - [x] Filter
  - [x] Projection
- [x] Create rule-based translator from logical to physical plan
- [x] Implement simple cost model for plan evaluation
- [x] Add statistics collection for better planning
  - [x] Table size statistics
  - [x] Column cardinality estimates
- [x] Connect operators to storage components
  - [x] TableScan using buffer pool and page manager
  - [ ] Index scan using B+Tree indexes
- [x] Add physical plan visualization tools
- [x] Write tests for physical planning

## Phase 4: Query Execution

- [x] Implement iterator-based execution model
- [x] Create execution context for operator state
- [x] Build execution engine to run physical plans
- [x] Implement result materialization
  - [x] Row representation
  - [x] Column access methods
- [ ] Add execution metrics collection
  - [ ] Execution time
  - [ ] Rows processed
  - [ ] Buffer pool interactions
- [x] Create utilities for result formatting
- [x] Implement comprehensive execution tests

## Phase 5: Advanced Features

- [x] Add JOIN operations
  - [x] NestedLoopJoin
  - [x] HashJoin
- [x] Implement aggregation operators
  - [x] GROUP BY support
  - [x] Aggregation functions (SUM, COUNT, AVG, MIN, MAX)
  - [x] HAVING clause support
- [ ] Add sorting and LIMIT support
  - [ ] In-memory sorting
  - [ ] External sorting for large datasets
- [x] Implement query optimization techniques
  - [x] Predicate pushdown
  - [ ] Join reordering
  - [ ] Index selection
- [ ] Support subqueries
- [ ] Add prepared statement support
- [ ] Enhance error handling and reporting
- [ ] Create benchmarks for query performance

## Immediate Next Steps

See the [project roadmap](../../project/roadmap.md) for current priorities and future work. 