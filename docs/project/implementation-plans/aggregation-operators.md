# Implementation Plan: Aggregation Operators

This document outlines the implementation plan for adding aggregate operator support to BayunDB, including GROUP BY, COUNT, SUM, AVG, MIN, and MAX functionality.

## 1. Research and Design (2 weeks)

### Parser and AST Enhancements
- Add lexer/parser support for GROUP BY clauses
- Add lexer/parser support for aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Design AST nodes for aggregate functions and GROUP BY clauses
- Update parser tests to include aggregate queries

### Query Planning
- Design logical representation of aggregation in query plans
- Design physical operators for aggregation (HashAggregate, SortAggregate)
- Update query planner to recognize and plan for aggregate operations

### Execution Engine
- Design data structures for managing grouped data
- Consider memory management for large group sets

## 2. Implementation (4 weeks)

### Phase 1: Parser Implementation (1 week)
- Extend lexer to recognize aggregate function keywords
- Implement parser rules for GROUP BY clauses
- Implement parser rules for HAVING clauses
- Build AST nodes for aggregate expressions
- Add validation for aggregate expressions in SELECT lists

### Phase 2: Logical Planning (1 week)
- Create logical plan nodes for aggregation
- Implement logical planning logic to handle GROUP BY
- Implement logical planning for HAVING clauses
- Add validation for group keys and aggregate functions
- Handle column references in GROUP BY (validate non-aggregated columns)

### Phase 3: Physical Planning (1 week)
- Implement HashAggregate operator
  - Group management with hash tables
  - Running aggregate calculation
- Implement SortAggregate operator (optional)
  - Sorting by group keys
  - Sequential group processing
- Add cost estimation for planner decisions

### Phase 4: Execution Engine (1 week)
- Implement aggregate function calculations:
  - COUNT: row counting
  - SUM: numeric addition
  - AVG: sum and count tracking
  - MIN/MAX: value comparison
- Add null handling for aggregate inputs
- Implement grouping logic with hash tables
- Implement HAVING predicate evaluation

## 3. Testing (2 weeks)

### Unit Testing
- Test individual aggregate function calculations
- Test grouping logic with various data types
- Test edge cases (empty groups, NULL values)

### Integration Testing
- Add test cases for each aggregate function
- Test GROUP BY with multiple columns
- Test HAVING clauses
- Test aggregate queries with JOINs
- Test queries with both WHERE and GROUP BY

### Performance Testing
- Benchmark memory usage for different group sizes
- Measure execution time for various aggregation scenarios
- Test with large datasets to identify bottlenecks

## 4. Documentation and Examples (1 week)

- Update SQL syntax documentation
- Add examples of aggregate queries
- Document limitations and edge cases
- Create tutorial for using aggregation in queries
- Update CLI examples to showcase aggregation capabilities

## 5. Code Review and Refinement (1 week)

- Address performance bottlenecks
- Refine memory management
- Ensure proper error handling
- Validate against SQL standards

## Timeline

- Total estimated time: 10 weeks
- Key milestones:
  - Week 2: Complete parser implementation with tests
  - Week 4: Logical and physical planning complete
  - Week 6: Execution engine implementation complete
  - Week 8: Integration tests passing
  - Week 10: Documentation complete and feature ready for release

## Dependencies

- Existing query execution framework
- Expression evaluation system
- Memory management infrastructure
- Test framework for query validation

## Sample Queries

This implementation will enable queries like:

```sql
-- Basic aggregation
SELECT department_id, COUNT(*), SUM(salary), AVG(salary)
FROM employees
GROUP BY department_id;

-- Aggregation with filtering
SELECT department_id, COUNT(*), AVG(salary)
FROM employees
WHERE status = 'active'
GROUP BY department_id
HAVING COUNT(*) > 5;

-- Multi-column grouping
SELECT department_id, job_title, COUNT(*)
FROM employees
GROUP BY department_id, job_title;

-- Aggregation with joins
SELECT d.name, COUNT(e.id), AVG(e.salary)
FROM employees e
JOIN departments d ON e.department_id = d.id
GROUP BY d.name;
```

## Implementation Considerations

### Memory Management
- For large datasets, investigate partial aggregation to reduce memory pressure
- Consider spilling to disk for groups that don't fit in memory

### Optimizations
- Push down filters before aggregation when possible
- Explore parallel aggregation for performance on multi-core systems
- Implement streaming aggregation for cases that don't require storing all groups 