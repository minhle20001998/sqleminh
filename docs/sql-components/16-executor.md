# Executor

## Purpose

The executor runs a plan and produces results.

## Why It Matters

The parser and planner only create instructions. The executor does the real work.

## Needed Behavior

Basic executors:

- `CreateTableExecutor`
- `InsertExecutor`
- `SeqScanExecutor`
- `FilterExecutor`
- `ProjectionExecutor`
- `UpdateExecutor`
- `DeleteExecutor`

## Example

For this SQL:

```sql
SELECT name FROM users WHERE id = 1;
```

The executor should:

1. Open a sequential scan on `users`
2. Decode each tuple into values
3. Evaluate `id = 1`
4. Return the `name` value for matching rows

## SQL Connection

The executor is where the SQL engine meets the existing storage code.

For example:

```text
SeqScanExecutor -> SequentialScan -> BufferPool -> DiskManager
```

