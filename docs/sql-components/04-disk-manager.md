# Disk Manager

## Purpose

The disk manager reads and writes pages to a database file.

In this project, `DiskManager` is implemented in `src/sql/storage/DiskManager.java`.

## Why It Matters

The database must persist data after the program exits. The disk manager is the component that talks to the file system.

## Current Behavior

The current `DiskManager` supports:

- Writing a page to disk
- Reading a page from disk
- Checking whether a page exists
- Expanding the database file when needed

Page offset is calculated like this:

```java
offset = pageId * pageSize
```

## Example

```java
DiskManager disk = new DiskManager("test.db", Page.PAGE_SIZE);

Page page = new Page(0, PageType.DATA);
page.insertRecord("Hello".getBytes());

disk.writePage(0, page.getData());

byte[] data = new byte[Page.PAGE_SIZE];
disk.readPage(0, data);

Page loaded = new Page(data);
System.out.println(new String(loaded.readRecord(0)));
```

## SQL Connection

When SQL inserts data:

```sql
INSERT INTO users VALUES (1, 'Minh');
```

The page containing that row must eventually be written to the `.db` file by the disk manager.

