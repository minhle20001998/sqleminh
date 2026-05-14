# Sequential Scan

## Purpose

A sequential scan reads records from table pages one by one.

In this project, `SequentialScan` is implemented in `src/sql/table/SequentialScan.java`.

## Why It Matters

The simplest way to execute a query is to scan every row and check whether it matches.

## Current Behavior

The current sequential scan:

- Starts at the first page
- Reads each slot
- Skips deleted slots
- Moves to the next page
- Stops after the last page

## Example

```java
SequentialScan scan = new SequentialScan(
    bufferPool,
    table.getFirstPageId(),
    table.getLastPageId()
);

byte[] record;
while ((record = scan.next()) != null) {
    System.out.println(new String(record));
}

scan.close();
```

## SQL Connection

This query can be implemented with a sequential scan:

```sql
SELECT * FROM users;
```

This query also starts as a sequential scan if no index exists:

```sql
SELECT * FROM users WHERE id = 1;
```

