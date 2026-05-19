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

The repo now has a basic tuple implementation in `src/sql/tuple`.

Implemented classes:

- `Value`
- `Tuple`

Tuple format supports:

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

Current binary layout:

```text
[ null bitmap ][ values in schema order ]
```

Rules:

- `INT` is stored as 4 little-endian bytes.
- `TEXT` is stored as 4-byte little-endian length, then UTF-8 bytes.
- `NULL` values set a bit in the null bitmap and store no value bytes.
- Null bitmap size is `(columnCount + 7) / 8`.

## SQL Connection

When the executor inserts a row, it should:

1. Validate values against the schema
2. Encode the row into bytes
3. Store those bytes using `TableHeap.insert(...)`

When selecting rows, it should:

1. Read bytes from `TableHeap`
2. Decode bytes using the schema
3. Return column values

## Current Java Example

```java
Schema usersSchema = new Schema(Arrays.asList(
    new Column("id", SqlType.INT, false),
    new Column("name", SqlType.TEXT, true)
));

Tuple tuple = new Tuple(usersSchema, Arrays.asList(
    Value.intValue(1),
    Value.textValue("Minh")
));

byte[] bytes = tuple.serialize();
Tuple loaded = Tuple.deserialize(usersSchema, bytes);
```

For `id = 1, name = "Minh"`, the record bytes are:

```text
00          null bitmap, no nulls
01 00 00 00 id = 1
04 00 00 00 text length = 4
4D 69 6E 68 "Minh"
```

For `id = 2, name = NULL`, the record bytes are:

```text
02          null bitmap, column 1 is null
02 00 00 00 id = 2
```
