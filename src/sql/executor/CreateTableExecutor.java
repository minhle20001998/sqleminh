package sql.executor;

import sql.catalog.Catalog;
import sql.plan.CreateTablePlan;
import sql.schema.Schema;
import sql.tuple.Tuple;

public class CreateTableExecutor implements Executor {
    private final Schema schema;
    private boolean done;

    public CreateTableExecutor(Catalog catalog, CreateTablePlan plan) throws Exception {
        catalog.createTable(plan.getTableName(), plan.getSchema());
        this.schema = plan.getSchema();
        this.done = false;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Tuple next() {
        if (done) {
            return null;
        }
        done = true;
        return null;
    }

    @Override
    public void close() throws Exception {
    }
}
