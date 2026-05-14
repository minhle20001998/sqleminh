# Predicate and Expression Engine

## Purpose

The expression engine evaluates SQL expressions.

Predicates are expressions that return true or false.

## Why It Matters

SQL needs more than just reading rows. It must compare, calculate, and filter values.

## Needed Behavior

Start with:

- Column references
- Literal values
- Equality: `=`
- Comparison: `<`, `>`, `<=`, `>=`
- Boolean operators: `AND`, `OR`, `NOT`

## Example SQL

```sql
SELECT * FROM users WHERE age >= 18 AND name = 'Minh';
```

## Example Expression Tree

```text
AND
  GreaterThanOrEqual
    ColumnRef(age)
    LiteralInt(18)
  Equal
    ColumnRef(name)
    LiteralText('Minh')
```

## SQL Connection

During a scan, each row is passed into the expression engine.

If the predicate returns `true`, the row is included in the result.

