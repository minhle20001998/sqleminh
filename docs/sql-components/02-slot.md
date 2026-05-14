# Slot

## Purpose

A slot is a small pointer to a record inside a page.

In this project, `Slot` is implemented in `src/sql/page/Slot.java`.

Each slot stores:

- `offset`: where the record starts in the page
- `length`: how many bytes the record uses

## Why It Matters

Rows may move or be deleted, but the database still needs a stable way to find records inside a page.

Instead of saying "record starts at byte 100", the system says "record is in slot 3". The slot then tells where the bytes are.

## Current Behavior

A deleted slot is represented by a negative offset:

```java
public boolean isDeleted() {
    return offset < 0;
}
```

## Example

```java
Slot slot = page.getSlot(0);

System.out.println(slot.getOffset());
System.out.println(slot.getLength());
```

## SQL Connection

For a row stored as:

```sql
users(id = 1, name = 'Minh')
```

The table stores the row bytes in a page, and the slot points to those bytes.

