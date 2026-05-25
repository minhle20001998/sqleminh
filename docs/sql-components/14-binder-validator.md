# Binder and Validator

## Purpose

The binder connects parsed SQL names to real database objects.

The validator checks whether the query is legal.

## Why It Matters

The parser may understand this SQL syntactically:

```sql
SELECT email FROM users;
```

But the binder must check whether:

- Table `users` exists
- Column `email` exists
- The expression types make sense

## Needed Behavior

The repo now has a binder/validator in `src/sql/binder`.

Implemented classes:

- `SqlBinder`
- `BoundStatement`
- `BoundCreateTableStatement`
- `BoundInsertStatement`
- `BoundSelectStatement`
- `BoundComparisonExpression`

The binder now:

- Resolve table names using the catalog
- Resolve column names using the table schema
- Attach column indexes/types to expressions
- Reject invalid SQL
- Reorder `INSERT` values into schema order when an explicit column list is used
- Validate literal types against column types
- Validate `NULL` against column nullability

## Example

Schema:

```text
users(id INT, name TEXT)
```

SQL:

```sql
SELECT email FROM users;
```

Result:

```text
Error: column email does not exist in table users
```

## SQL Connection

Binding happens after parsing and before planning.

It turns a query from "looks like SQL" into "refers to real tables and columns".

## Current Java Example

```java
SqlParser parser = new SqlParser();
SqlBinder binder = new SqlBinder(catalog);

SqlStatement parsed = parser.parse(
    "INSERT INTO users (name, id) VALUES ('Minh', 1);"
);

BoundStatement bound = binder.bind(parsed);
```

The bound insert stores values in schema order:

```text
schema: id, name
query:  name, id
bound:  id = 1, name = 'Minh'
```

This means the future executor can directly create a `Tuple` from the bound insert values.
