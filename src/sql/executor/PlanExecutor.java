package sql.executor;

import sql.buffer.BufferPool;
import sql.catalog.Catalog;
import sql.plan.CreateTablePlan;
import sql.plan.FilterPlan;
import sql.plan.InsertPlan;
import sql.plan.LogicalPlan;
import sql.plan.ProjectionPlan;
import sql.plan.SeqScanPlan;

public class PlanExecutor {
    private final Catalog catalog;
    private final BufferPool bufferPool;

    public PlanExecutor(Catalog catalog, BufferPool bufferPool) {
        this.catalog = catalog;
        this.bufferPool = bufferPool;
    }

    public Executor build(LogicalPlan plan) throws Exception {
        if (plan instanceof CreateTablePlan p) {
            return new CreateTableExecutor(catalog, p);
        }

        if (plan instanceof InsertPlan p) {
            return new InsertExecutor(catalog, p);
        }

        if (plan instanceof SeqScanPlan p) {
            return new SeqScanExecutor(catalog, bufferPool, p);
        }

        if (plan instanceof FilterPlan p) {
            Executor child = build(p.getInput());
            return new FilterExecutor(child, p.getPredicate());
        }

        if (plan instanceof ProjectionPlan p) {
            Executor child = build(p.getInput());
            return new ProjectionExecutor(child, p.getColumnIndexes(), p.getColumnNames());
        }

        throw new IllegalArgumentException("Unsupported plan: " + plan.getClass().getSimpleName());
    }
}
