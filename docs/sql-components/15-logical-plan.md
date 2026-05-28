# Logical Plan

## Purpose

A logical plan describes what the database should do, without deciding all low-level details.

## Why It Matters

SQL is declarative. The user says what result they want, not exactly how to get it.

The planner turns SQL into steps.

## Needed Behavior

The repo now has a logical planner in `src/sql/plan`.

Implemented classes:

- `LogicalPlanner`
- `LogicalPlan`
- `CreateTablePlan`
- `InsertPlan`
- `SeqScanPlan`
- `FilterPlan`
- `ProjectionPlan`

For basic SQL, logical plans now include:

- Create table
- Insert row
- Sequential scan
- Filter
- Projection

Not implemented yet:

- Update
- Delete
- index scans
- optimization

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

Current planner rule for `SELECT`:

```text
Start with SeqScan(table)
If WHERE exists, wrap with Filter(predicate)
If SELECT is not *, wrap with Projection(columns)
```

Examples:

```sql
SELECT * FROM users;
```

```text
SeqScan(users)
```

```sql
SELECT name FROM users WHERE id = 1;
```

```text
Projection(name)
  Filter(id = 1)
    SeqScan(users)
```

## SQL Connection

The executor can run this plan from bottom to top:

1. Scan `users`
2. Keep only rows where `id = 1`
3. Return only the `name` column
