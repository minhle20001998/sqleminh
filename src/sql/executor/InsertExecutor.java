package sql.executor;

import sql.catalog.Catalog;
import sql.catalog.TableMetadata;
import sql.plan.InsertPlan;
import sql.schema.Schema;
import sql.table.TableHeap;
import sql.tuple.Tuple;

public class InsertExecutor implements Executor {
    private final Schema schema;
    private boolean done;

    public InsertExecutor(Catalog catalog, InsertPlan plan) throws Exception {
        TableMetadata metadata = plan.getTableMetadata();
        TableHeap tableHeap = catalog.getTableHeap(metadata.getTableName());

        Tuple tuple = new Tuple(metadata.getSchema(), plan.getValuesInSchemaOrder());
        tableHeap.insert(tuple.serialize());

        this.schema = metadata.getSchema();
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
