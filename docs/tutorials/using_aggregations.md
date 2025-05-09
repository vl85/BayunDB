# Using Aggregation Functions in BayunDB

This tutorial covers how to use SQL aggregation functions in BayunDB to perform calculations on sets of rows and produce summary results.

## Prerequisites

- BayunDB installed and running
- Basic understanding of SQL syntax
- BayunDB CLI (`bnql`) available

## Overview of Aggregation Functions

Aggregation functions perform calculations on a set of rows and return a single result. BayunDB supports the following aggregation functions:

- `COUNT`: Counts the number of rows or non-NULL values
- `SUM`: Calculates the sum of values in a column
- `AVG`: Calculates the average of values in a column
- `MIN`: Finds the minimum value in a column
- `MAX`: Finds the maximum value in a column

## Basic Aggregation

### Counting Rows

The simplest use of an aggregate function is to count the total number of rows in a table:

```sql
SELECT COUNT(*) FROM users;
```

Example output:
```
| COUNT(*) |
+----------+
| 10       |
(1 row)
```

You can also count non-NULL values in a specific column:

```sql
SELECT COUNT(email) FROM users;
```

This will count only rows where the email column is not NULL.

### Finding Minimum and Maximum Values

To find the minimum and maximum values in a column:

```sql
SELECT MIN(age), MAX(age) FROM users;
```

Example output:
```
| MIN(age) | MAX(age) |
+----------+----------+
| 18       | 65       |
(1 row)
```

### Calculating Sums and Averages

To calculate the sum and average of numeric columns:

```sql
SELECT SUM(amount), AVG(amount) FROM orders;
```

Example output:
```
| SUM(amount) | AVG(amount) |
+-------------+-------------+
| 12500.50    | 125.00      |
(1 row)
```

## Grouping Data

Aggregation becomes more powerful when combined with the `GROUP BY` clause, which allows you to perform calculations on groups of rows.

### Basic Grouping

To count the number of users in each department:

```sql
SELECT department_id, COUNT(*) 
FROM employees 
GROUP BY department_id;
```

Example output:
```
| department_id | COUNT(*) |
+---------------+----------+
| 1             | 5        |
| 2             | 8        |
| 3             | 3        |
(3 rows)
```

### Multiple Grouping Columns

You can group by multiple columns to create more specific groupings:

```sql
SELECT department_id, job_title, COUNT(*) 
FROM employees 
GROUP BY department_id, job_title;
```

Example output:
```
| department_id | job_title    | COUNT(*) |
+---------------+--------------+----------+
| 1             | "Developer"  | 3        |
| 1             | "Manager"    | 1        |
| 1             | "Tester"     | 1        |
| 2             | "Developer"  | 5        |
| 2             | "Manager"    | 1        |
| 2             | "Designer"   | 2        |
| 3             | "Developer"  | 2        |
| 3             | "Manager"    | 1        |
(8 rows)
```

### Multiple Aggregate Functions

You can use multiple aggregate functions in a single query:

```sql
SELECT department_id, 
       COUNT(*) as employee_count,
       AVG(salary) as avg_salary,
       SUM(salary) as total_payroll,
       MIN(salary) as lowest_salary,
       MAX(salary) as highest_salary
FROM employees
GROUP BY department_id;
```

Example output:
```
| department_id | employee_count | avg_salary | total_payroll | lowest_salary | highest_salary |
+---------------+----------------+------------+---------------+---------------+----------------+
| 1             | 5              | 75000.00   | 375000.00     | 60000.00      | 120000.00      |
| 2             | 8              | 68000.00   | 544000.00     | 55000.00      | 110000.00      |
| 3             | 3              | 80000.00   | 240000.00     | 65000.00      | 125000.00      |
(3 rows)
```

## Filtering Groups with HAVING

The `HAVING` clause allows you to filter groups after aggregation, similar to how the `WHERE` clause filters individual rows before aggregation.

To find departments with more than 5 employees:

```sql
SELECT department_id, COUNT(*) 
FROM employees 
GROUP BY department_id 
HAVING COUNT(*) > 5;
```

Example output:
```
| department_id | COUNT(*) |
+---------------+----------+
| 2             | 8        |
(1 row)
```

You can combine `WHERE` and `HAVING` in the same query:

```sql
SELECT department_id, COUNT(*) 
FROM employees 
WHERE status = 'active'
GROUP BY department_id 
HAVING COUNT(*) > 3;
```

This will first filter for active employees, then group them by department, and finally show only departments with more than 3 active employees.

## Filtering with Aggregate Expressions

You can also use aggregate functions in the `HAVING` clause with other aggregates:

```sql
SELECT department_id, COUNT(*), AVG(salary)
FROM employees
GROUP BY department_id
HAVING AVG(salary) > 70000;
```

Example output:
```
| department_id | COUNT(*) | AVG(salary) |
+---------------+----------+-------------+
| 1             | 5        | 75000.00    |
| 3             | 3        | 80000.00    |
(2 rows)
```

## Combining with JOINs

Aggregation functions work well with JOINs to analyze related data:

```sql
SELECT d.name as department, COUNT(e.id) as employee_count
FROM employees e
JOIN departments d ON e.department_id = d.id
GROUP BY d.name;
```

Example output:
```
| department   | employee_count |
+--------------+----------------+
| "Engineering"| 5              |
| "Marketing"  | 8              |
| "Sales"      | 3              |
(3 rows)
```

## Common Errors and Troubleshooting

- **Mixing aggregate and non-aggregate columns**: All non-aggregated columns in the SELECT list must appear in the GROUP BY clause.

    *Incorrect*:
    ```sql
    SELECT department_id, job_title, COUNT(*)
    FROM employees
    GROUP BY department_id;
    ```

    *Correct*:
    ```sql
    SELECT department_id, job_title, COUNT(*)
    FROM employees
    GROUP BY department_id, job_title;
    ```

- **Using WHERE instead of HAVING for aggregate filtering**: The WHERE clause cannot contain aggregate functions.

    *Incorrect*:
    ```sql
    SELECT department_id, COUNT(*)
    FROM employees
    WHERE COUNT(*) > 5
    GROUP BY department_id;
    ```

    *Correct*:
    ```sql
    SELECT department_id, COUNT(*)
    FROM employees
    GROUP BY department_id
    HAVING COUNT(*) > 5;
    ```

## Conclusion

Aggregation functions are powerful tools for analyzing and summarizing data in BayunDB. By combining these functions with GROUP BY and HAVING clauses, you can extract valuable insights from your data.

For more advanced uses, including complex expressions and subqueries, refer to the BayunDB SQL Reference Guide. 