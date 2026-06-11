package sql.executor;

import sql.schema.Column;
import sql.schema.Schema;
import sql.tuple.Tuple;
import sql.tuple.Value;

import java.util.ArrayList;
import java.util.List;

public class ProjectionExecutor implements Executor {
    private final Executor child;
    private final Schema projectedSchema;
    private final List<Integer> columnIndexes;

    public ProjectionExecutor(Executor child, List<Integer> columnIndexes, List<String> columnNames) {
        this.child = child;
        this.columnIndexes = columnIndexes;

        Schema childSchema = child.getSchema();
        List<Column> projectedColumns = new ArrayList<>();

        for (int i = 0; i < columnIndexes.size(); i++) {
            int colIndex = columnIndexes.get(i);
            Column original = childSchema.getColumn(colIndex);
            projectedColumns.add(new Column(columnNames.get(i), original.getType(), original.isNullable()));
        }

        this.projectedSchema = new Schema(projectedColumns);
    }

    @Override
    public Schema getSchema() {
        return projectedSchema;
    }

    @Override
    public Tuple next() throws Exception {
        Tuple tuple = child.next();
        if (tuple == null) {
            return null;
        }

        List<Value> projectedValues = new ArrayList<>();
        for (int index : columnIndexes) {
            projectedValues.add(tuple.getValue(index));
        }

        return new Tuple(projectedSchema, projectedValues);
    }

    @Override
    public void close() throws Exception {
        child.close();
    }
}
