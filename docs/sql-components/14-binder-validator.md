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

The binder should:

- Resolve table names using the catalog
- Resolve column names using the table schema
- Attach column indexes/types to expressions
- Reject invalid SQL

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

