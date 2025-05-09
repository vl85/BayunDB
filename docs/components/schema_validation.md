# Schema Validation and Type Checking in BayunDB

This document explains the schema validation and type checking system implemented in BayunDB, which ensures data integrity and type safety during query execution.

## Overview

The type validation system in BayunDB provides several key functions:

1. **Value Validation**: Ensures values match their column's defined data type
2. **Row Validation**: Verifies all columns in a row satisfy type and constraint requirements
3. **Expression Type Checking**: Determines the result type of expressions and validates type compatibility
4. **Type Conversions**: Safely converts values between compatible types

## Components

### Type Validator

The `TypeValidator` is the core component that provides validation functions:

- `validate_value`: Checks if a value matches a column's data type
- `validate_row`: Validates an entire row against a table schema
- `check_type_compatibility`: Determines if two types are compatible for an operation
- `get_expression_type`: Computes the result type of an SQL expression
- `convert_value`: Converts between parser and executor value representations
- `convert_to_type`: Attempts to convert a value to another data type

### Validation Errors

Validation failures are represented by the `ValidationError` enum, which includes:

- `TypeMismatch`: Value doesn't match the expected type
- `NullValueNotAllowed`: NULL value in a non-nullable column
- `InvalidOperation`: Operation not supported for the given operands
- `ColumnNotFound`: Referenced column doesn't exist
- `TableNotFound`: Referenced table doesn't exist
- `IncompatibleTypes`: Types not compatible for the operation
- `ValueOutOfRange`: Value outside the valid range for the target type
- `InvalidDateFormat`: Incorrect date string format
- `InvalidTimestampFormat`: Incorrect timestamp string format

## Implementation Details

### Data Type Compatibility

BayunDB defines compatibility rules for operations between different data types:

#### Arithmetic Operations (+, -, *, /, %)

| Left Type | Right Type | Result Type | Notes |
|-----------|------------|-------------|-------|
| Integer   | Integer    | Integer     | |
| Integer   | Float      | Float       | Integers are promoted to floats |
| Float     | Integer    | Float       | Integers are promoted to floats |
| Float     | Float      | Float       | |
| Text      | Text       | Text        | Only for + (concatenation) |

#### Comparison Operations (=, !=, <, >, <=, >=)

| Left Type | Right Type | Result Type | Notes |
|-----------|------------|-------------|-------|
| Any Type  | Same Type  | Boolean     | Types must match exactly, except for numeric types |
| Integer   | Float      | Boolean     | Numeric types can be compared |
| Float     | Integer    | Boolean     | Numeric types can be compared |

#### Logical Operations (AND, OR)

| Left Type | Right Type | Result Type | Notes |
|-----------|------------|-------------|-------|
| Boolean   | Boolean    | Boolean     | Both operands must be boolean |

### Type Conversion Rules

BayunDB supports automatic type conversion in certain cases:

| From Type | To Type  | Behavior |
|-----------|----------|----------|
| Integer   | Float    | Simple conversion |
| Integer   | Text     | String representation |
| Integer   | Boolean  | 0 = false, non-0 = true |
| Float     | Integer  | Truncation (if no fractional part and within range) |
| Float     | Text     | String representation |
| Float     | Boolean  | 0.0 = false, non-0 = true |
| Text      | Integer  | Parse string as integer |
| Text      | Float    | Parse string as float |
| Text      | Boolean  | "true", "t", "yes", "y", "1" = true; "false", "f", "no", "n", "0" = false |
| Boolean   | Integer  | false = 0, true = 1 |
| Boolean   | Float    | false = 0.0, true = 1.0 |
| Boolean   | Text     | "true" or "false" |

## Integration with Query Execution

The type validation system is integrated with query execution in the following ways:

1. **Expression Evaluation**: When evaluating expressions (e.g., in WHERE clauses), the executor checks operand types
2. **Operator Implementation**: Operators like Filter validate expression types during execution
3. **Data Insertion**: Before inserting data, values are validated against the table schema
4. **Type Conversions**: When needed, values are converted between compatible types

## Example Usage

```rust
// Validate a value against a column type
let value = DataValue::Integer(42);
let column = table.get_column("age").unwrap();
TypeValidator::validate_value(&value, &column)?;

// Validate an entire row against a table schema
let values: Vec<(&String, &DataValue)> = row.values_with_names().collect();
TypeValidator::validate_row(&values, &table)?;

// Get the result type of an expression
let expr_type = TypeValidator::get_expression_type(&expression, &table)?;

// Convert a value to a different type
let text_value = DataValue::Text("42".to_string());
let int_value = TypeValidator::convert_to_type(text_value, &DataType::Integer)?;
```

## Future Enhancements

1. **Advanced Constraint Validation**: Add support for CHECK constraints and foreign key constraints
2. **Optimized Type Checking**: Add type inference and precomputed type information for expressions
3. **Custom Data Types**: Support for user-defined types and domain constraints
4. **Type Coercion Rules**: More sophisticated type promotion and coercion rules for complex expressions 