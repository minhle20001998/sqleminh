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

The repo now has a small hand-written parser in `src/sql/parser`.

Implemented statement types:

- `CREATE TABLE`
- `INSERT`
- `SELECT`

Not implemented yet:

- `UPDATE`
- `DELETE`
- joins
- order by
- aggregates
- complex expressions

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

## Current Supported SQL

Create table:

```sql
CREATE TABLE users (id INT NOT NULL, name TEXT NULL);
```

Insert with schema order:

```sql
INSERT INTO users VALUES (1, 'Minh');
```

Insert with explicit column list:

```sql
INSERT INTO users (name, id) VALUES ('Minh', 1);
```

Select all:

```sql
SELECT * FROM users;
```

Select columns with simple equality:

```sql
SELECT id, name FROM users WHERE id = 1;
```

String literals use single quotes. Escaped single quote uses two quotes:

```sql
INSERT INTO users VALUES (1, 'Minh''s row');
```

## SQL Connection

The parser is the first step after the user enters SQL.

It does not read pages or execute queries. It only understands SQL syntax.
