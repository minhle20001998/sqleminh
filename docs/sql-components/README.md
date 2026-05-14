# Basic SQL Engine Components

This folder explains the components needed to turn this project from a page/table storage prototype into a basic SQL database.

The project already has several storage-layer components. The missing pieces are mostly above the storage layer: schema, catalog, tuple encoding, parser, planner, executor, and query features.

## Component Count

Existing components in this repo: **8**

Planned components for basic SQL behavior: **10**

Total components covered: **18**

## Existing Components

1. [Page](01-page.md)
2. [Slot](02-slot.md)
3. [Page Type](03-page-type.md)
4. [Disk Manager](04-disk-manager.md)
5. [Frame](05-frame.md)
6. [Buffer Pool](06-buffer-pool.md)
7. [Record ID](07-record-id.md)
8. [Table Heap](08-table-heap.md)
9. [Sequential Scan](09-sequential-scan.md)

Note: `SequentialScan` is already implemented, so the implemented count is effectively 9 if scan is counted separately from table access.

## Planned Components

10. [Catalog and Metadata](10-catalog-metadata.md)
11. [Schema](11-schema.md)
12. [Tuple and Row Format](12-tuple-row-format.md)
13. [SQL Parser](13-sql-parser.md)
14. [Binder and Validator](14-binder-validator.md)
15. [Logical Plan](15-logical-plan.md)
16. [Executor](16-executor.md)
17. [Predicate and Expression Engine](17-predicate-expression-engine.md)
18. [Index](18-index.md)
19. [Transaction and Recovery](19-transaction-recovery.md)
20. [Automated Tests](20-automated-tests.md)

For study order, start with `Catalog and Metadata`, then `Schema`, then `Tuple and Row Format`. Those unlock meaningful `CREATE TABLE`, `INSERT`, and `SELECT`.

