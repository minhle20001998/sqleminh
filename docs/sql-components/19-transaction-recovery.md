# Transaction and Recovery

## Purpose

Transactions group changes together.

Recovery protects the database from crashes.

## Why It Matters

Without transactions, the database can end up with half-finished changes.

## Needed Behavior

Basic transaction features:

- Begin
- Commit
- Rollback

Basic recovery features:

- Write-ahead log
- Redo committed changes after crash
- Undo uncommitted changes after crash

## Example SQL

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

Both updates should succeed together. If one fails, neither should remain.

## SQL Connection

Transactions are not required for the first working SQL prototype, but they are required for a reliable real database.

For study, implement this after basic `CREATE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE`.

