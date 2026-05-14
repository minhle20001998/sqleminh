# Frame

## Purpose

A frame is a wrapper around one page in memory.

In this project, `Frame` is implemented in `src/sql/buffer/Frame.java`.

Each frame tracks:

- The loaded `Page`
- Pin count
- Dirty state

## Why It Matters

The buffer pool keeps pages in memory. A frame stores the page plus metadata needed for memory management.

## Current Behavior

The current frame supports:

- Pinning a page
- Unpinning a page
- Marking a page dirty
- Clearing dirty state
- Checking if a page is pinned

## Important Concepts

Pin count means how many users are currently using the page.

Dirty means the page was changed in memory and must be written back to disk.

## Example

```java
Frame frame = new Frame(page);

frame.pin();
frame.markDirty();
frame.unpin();
```

## SQL Connection

During a query, many parts of the engine may read or write pages. Pinning prevents the buffer pool from evicting a page while it is still being used.

