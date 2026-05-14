# Index

## Purpose

An index helps the database find rows without scanning the whole table.

## Why It Matters

Sequential scan is simple but slow for large tables.

An index stores a mapping from column value to record location.

## Needed Behavior

A basic index should support:

- Insert key and `RecordId`
- Search by key
- Delete key
- Update key

## Example

For table:

```sql
CREATE TABLE users (id INT, name TEXT);
```

Index:

```sql
CREATE INDEX idx_users_id ON users(id);
```

Index data:

```text
1 -> RecordId(pageId = 0, slotId = 0)
2 -> RecordId(pageId = 0, slotId = 1)
```

## SQL Connection

This query:

```sql
SELECT * FROM users WHERE id = 2;
```

Can use the index to jump directly to the matching row.

Without an index, the database must scan every row.

## Later Implementation

A real database usually uses a B+Tree index. For learning, a simple persisted sorted structure or in-memory map can come first.

