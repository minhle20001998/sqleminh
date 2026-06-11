package sql.executor;

import sql.buffer.BufferPool;
import sql.catalog.Catalog;
import sql.catalog.TableMetadata;
import sql.plan.SeqScanPlan;
import sql.schema.Schema;
import sql.table.SequentialScan;
import sql.tuple.Tuple;

public class SeqScanExecutor implements Executor {
    private final Schema schema;
    private final SequentialScan scan;

    public SeqScanExecutor(Catalog catalog, BufferPool bufferPool, SeqScanPlan plan) throws Exception {
        TableMetadata metadata = plan.getTableMetadata();
        catalog.getTableMetadata(metadata.getTableName());

        this.schema = metadata.getSchema();
        this.scan = new SequentialScan(bufferPool, metadata.getFirstPageId(), metadata.getLastPageId());
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Tuple next() throws Exception {
        byte[] raw = scan.next();
        if (raw == null) {
            return null;
        }
        return Tuple.deserialize(schema, raw);
    }

    @Override
    public void close() throws Exception {
        scan.close();
    }
}
