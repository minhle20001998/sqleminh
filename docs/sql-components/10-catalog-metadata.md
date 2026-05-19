# Catalog and Metadata

## Purpose

The catalog stores database metadata.

Metadata is data about the database itself, such as:

- Table names
- Column definitions
- First page id of each table
- Last page id of each table
- Index definitions

## Why It Matters

Right now, `TableHeap` receives `firstPageId` from code and keeps `lastPageId` only in memory. After the program restarts, the database does not truly know the table layout.

A real SQL database must remember this information.

## Current Implementation

The repo now has a basic persistent catalog.

Catalog metadata is stored in page `0`:

```text
Page 0 -> catalog metadata
Page 1 and above -> table data pages
```

Current table ranges are reserved in blocks of `1000` pages:

```text
users: page 1 through page 1000
posts: page 1001 through page 2000
```

Each table stores:

- Table name
- Schema
- First page id
- Last page id
- Max page id

The catalog can be loaded again after reopening the database file.

Call this before shutdown to persist updated table page metadata:

```java
catalog.flush();
bufferPool.flushAll();
```

## Needed Behavior Later

The catalog should support:

- Create table metadata
- Look up table metadata by name
- Persist table metadata to disk
- Load metadata when the database starts
- Track table heap page ids

## Example Metadata

```text
table: users
columns:
  id INT
  name TEXT
firstPageId: 1
lastPageId: 3
maxPageId: 1000
```

The current catalog stores each table entry in page `0` as text:

```text
tableName|firstPageId|lastPageId|maxPageId|schema
```

Example schema field:

```text
id:INT:false,name:TEXT:true
```

## SQL Connection

This SQL:

```sql
CREATE TABLE users (id INT, name TEXT);
```

Should create catalog metadata for the `users` table.

This SQL:

```sql
SELECT * FROM users;
```

Should ask the catalog where the `users` table is stored.
