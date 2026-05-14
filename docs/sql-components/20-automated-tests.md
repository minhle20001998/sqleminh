# Automated Tests

## Purpose

Automated tests verify that database behavior stays correct as the project grows.

## Why It Matters

The current project uses manual test methods in `Main.java`. That is useful for learning, but SQL behavior will become too complex for manual testing.

## Needed Behavior

Tests should cover:

- Page insert/read/delete/update
- Disk persistence
- Buffer pool eviction
- Table heap insert/read/update/delete
- Sequential scan
- Schema validation
- Tuple encoding and decoding
- SQL parsing
- SQL execution

## Example Test Cases

```text
INSERT then SELECT returns inserted row
DELETE removes row from scan result
UPDATE shorter value overwrites in place
UPDATE longer value moves record
SELECT with WHERE returns only matching rows
```

## SQL Connection

Every SQL feature should have tests.

Example:

```sql
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'Minh');
SELECT name FROM users WHERE id = 1;
```

Expected result:

```text
Minh
```

