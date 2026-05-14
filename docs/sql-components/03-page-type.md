# Page Type

## Purpose

Page type identifies what kind of data a page contains.

In this project, `PageType` is implemented in `src/sql/page/PageType.java`.

Current page types:

- `DATA`
- `INDEX`
- `META`

## Why It Matters

A database file contains many pages. Not all pages store rows.

Some pages may store:

- Table rows
- Index data
- Database metadata
- Free-space information

The page type helps the database interpret the bytes correctly.

## Example

```java
Page page = new Page(0, PageType.DATA);

if (page.getPageType() == PageType.DATA) {
    System.out.println("This page stores table rows.");
}
```

## SQL Connection

For this SQL:

```sql
CREATE INDEX idx_users_id ON users(id);
```

The database would eventually need `INDEX` pages to store index structures separately from normal `DATA` pages.

