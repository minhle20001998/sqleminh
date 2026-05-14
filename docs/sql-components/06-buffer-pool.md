# Buffer Pool

## Purpose

The buffer pool caches disk pages in memory.

In this project, `BufferPool` is implemented in `src/sql/buffer/BufferPool.java`.

## Why It Matters

Disk is slow. Memory is faster. A database uses a buffer pool so frequently used pages do not need to be read from disk again and again.

## Current Behavior

The current buffer pool supports:

- Fetching a page
- Creating a page if it does not exist
- Pinning and unpinning pages
- Marking pages dirty
- Flushing one dirty page
- Flushing all dirty pages
- FIFO eviction

## Example

```java
BufferPool bufferPool = new BufferPool(2, diskManager);

Page page = bufferPool.fetchPage(0);
page.insertRecord("Hello".getBytes());
bufferPool.unpinPage(0, true);

bufferPool.flushAll();
```

## SQL Connection

For this query:

```sql
SELECT * FROM users;
```

The executor asks the buffer pool for table pages. The buffer pool decides whether to return the page from memory or load it from disk.

