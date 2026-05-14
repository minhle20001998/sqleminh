# Record ID

## Purpose

A record id identifies exactly where a record lives.

In this project, `RecordId` is implemented in `src/sql/record/RecordId.java`.

It contains:

- `pageId`
- `slotId`

## Why It Matters

The database needs a stable identifier for a row inside the storage layer.

## Current Behavior

When `TableHeap.insert(...)` inserts a record, it returns a `RecordId`.

That `RecordId` can later be used for:

- Reading
- Updating
- Deleting

## Example

```java
RecordId rid = table.insert("Hello".getBytes());

byte[] data = table.read(rid);
table.delete(rid);
```

## SQL Connection

An index can store a mapping like:

```text
id = 10 -> RecordId(pageId = 3, slotId = 7)
```

Then the database can quickly find the row without scanning the whole table.

