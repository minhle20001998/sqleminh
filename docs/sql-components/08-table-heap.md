# Table Heap

## Purpose

A table heap stores table records across data pages.

In this project, `TableHeap` is implemented in `src/sql/table/TableHeap.java`.

## Why It Matters

The table heap is the main storage structure for table rows.

## Current Behavior

The current table heap supports:

- Insert
- Read
- Update
- Delete

It starts from `firstPageId` and inserts into the first page with enough space. If no space exists, it creates a new page.

The table heap can also have a `maxPageId`. If the table reaches this page and still needs more space, insert throws an error instead of growing into another table's page range.

## Example

```java
TableHeap table = new TableHeap(bufferPool, 0);

RecordId rid = table.insert("Hello".getBytes());
byte[] data = table.read(rid);

System.out.println(new String(data));
```

## Update Behavior

If the new record fits in the old slot, it overwrites in place.

If the new record is larger, the old slot is deleted and the new record is inserted elsewhere.

## Page Range

With catalog support, a table owns a page range:

```text
firstPageId ... maxPageId
```

Example:

```text
users: page 1 through page 1000
posts: page 1001 through page 2000
```

If `users` fills page `1000`, the next insert fails instead of using page `1001`.

## SQL Connection

This SQL:

```sql
UPDATE users SET name = 'Longer Name' WHERE id = 1;
```

Eventually becomes a table heap update for one or more matching records.
