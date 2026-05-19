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

The repo now has a basic schema implementation in `src/sql/schema`.

Implemented classes:

- `SqlType`
- `Column`
- `Schema`

The schema supports:

- Defining columns
- Looking up a column by name
- Checking data types
- Serializing/deserializing schema metadata for the catalog

Tuple serialization is not implemented yet. That is the next component.

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

## Current Java Example

```java
Schema usersSchema = new Schema(Arrays.asList(
    new Column("id", SqlType.INT, false),
    new Column("name", SqlType.TEXT, true)
));

catalog.createTable("users", usersSchema);
```

The catalog persists this schema along with the table page metadata.

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
