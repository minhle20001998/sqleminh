# Tuple and Row Format

## Purpose

A tuple is one table row stored in a structured binary format.

The row format defines how column values become bytes.

## Why It Matters

The storage layer only understands bytes. SQL works with typed values.

The tuple layer converts between:

```text
id = 1, name = 'Minh'
```

and:

```text
binary bytes stored inside a page
```

## Needed Behavior

Tuple format should support:

- Fixed-length values such as `INT`
- Variable-length values such as `TEXT`
- Null values
- Reading one column without guessing byte positions
- Reconstructing a row from bytes

## Simple Format Example

For table:

```sql
CREATE TABLE users (id INT, name TEXT);
```

Row:

```sql
INSERT INTO users VALUES (1, 'Minh');
```

Possible binary layout:

```text
null bitmap
id: 4 bytes
name length: 4 bytes
name bytes: 4 bytes
```

## SQL Connection

When the executor inserts a row, it should:

1. Validate values against the schema
2. Encode the row into bytes
3. Store those bytes using `TableHeap.insert(...)`

When selecting rows, it should:

1. Read bytes from `TableHeap`
2. Decode bytes using the schema
3. Return column values

