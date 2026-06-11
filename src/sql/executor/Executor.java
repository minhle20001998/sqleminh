package sql.executor;

import sql.schema.Schema;
import sql.tuple.Tuple;

public interface Executor {
    Schema getSchema();

    Tuple next() throws Exception;

    void close() throws Exception;
}
