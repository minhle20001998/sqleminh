# Page

## Purpose

A page is the smallest fixed-size block of data that the database reads from or writes to disk.

In this project, `Page` is implemented in `src/sql/page/Page.java`.

Each page is `4096` bytes. It stores:

- Page header
- Record bytes
- Slot directory

The page is responsible for placing records inside its byte array.

## Why It Matters

Real databases do not usually read and write individual rows directly. They read and write pages. Pages make disk access predictable and efficient.

## Current Behavior

The current `Page` supports:

- Creating a new empty page
- Loading a page from disk bytes
- Inserting raw record bytes
- Reading a record by slot id
- Updating a record in place if the new data fits
- Deleting a record by marking its slot as deleted
- Checking free space

## Example

```java
Page page = new Page(0, PageType.DATA);

int slotId = page.insertRecord("Hello".getBytes());
byte[] record = page.readRecord(slotId);

System.out.println(new String(record)); // Hello
```

## SQL Connection

When SQL runs this:

```sql
INSERT INTO users VALUES (1, 'Minh');
```

The database eventually turns that row into bytes and stores those bytes inside a page.

