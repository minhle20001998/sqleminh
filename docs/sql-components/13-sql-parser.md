# SQL Parser

## Purpose

The SQL parser converts SQL text into a structured representation.

## Why It Matters

The database cannot execute raw text directly.

It must convert this:

```sql
SELECT name FROM users WHERE id = 1;
```

into an object tree the engine can understand.

## Needed Behavior

Start with a small parser that supports:

- `CREATE TABLE`
- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`

## Example Input

```sql
SELECT name FROM users WHERE id = 1;
```

## Example Parsed Output

```text
SelectStatement
  columns: [name]
  table: users
  where:
    Equal
      left: ColumnRef(id)
      right: LiteralInt(1)
```

## SQL Connection

The parser is the first step after the user enters SQL.

It does not read pages or execute queries. It only understands SQL syntax.

