# Schema

## Purpose

A schema describes the structure of a table.

It defines:

- Column names
- Column types
- Column order
- Nullability

## Why It Matters

The current project stores raw `byte[]` records. SQL needs named and typed values.

Without schemas, the database cannot understand this:

```sql
SELECT name FROM users WHERE id = 1;
```

## Needed Behavior

A schema should support:

- Defining columns
- Looking up a column by name
- Checking data types
- Knowing how to serialize and deserialize rows

## Example

```text
Table: users

Columns:
0: id INT NOT NULL
1: name TEXT
2: age INT
```

## Possible Java Shape

```java
public class Schema {
    private final List<Column> columns;
}

public class Column {
    private final String name;
    private final SqlType type;
    private final boolean nullable;
}
```

## SQL Connection

For this statement:

```sql
INSERT INTO users (id, name) VALUES (1, 'Minh');
```

The schema checks that:

- `id` exists
- `name` exists
- `1` can be stored as `INT`
- `'Minh'` can be stored as `TEXT`

