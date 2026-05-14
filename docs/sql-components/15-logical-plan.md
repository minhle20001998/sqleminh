# Logical Plan

## Purpose

A logical plan describes what the database should do, without deciding all low-level details.

## Why It Matters

SQL is declarative. The user says what result they want, not exactly how to get it.

The planner turns SQL into steps.

## Needed Behavior

For basic SQL, logical plans can include:

- Create table
- Insert row
- Sequential scan
- Filter
- Projection
- Update
- Delete

## Example SQL

```sql
SELECT name FROM users WHERE id = 1;
```

## Example Logical Plan

```text
Projection(name)
  Filter(id = 1)
    SequentialScan(users)
```

## SQL Connection

The executor can run this plan from bottom to top:

1. Scan `users`
2. Keep only rows where `id = 1`
3. Return only the `name` column

