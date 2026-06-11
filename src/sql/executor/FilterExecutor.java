package sql.executor;

import sql.binder.BoundComparisonExpression;
import sql.schema.Schema;
import sql.tuple.Tuple;

public class FilterExecutor implements Executor {
    private final Executor child;
    private final BoundComparisonExpression predicate;

    public FilterExecutor(Executor child, BoundComparisonExpression predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public Schema getSchema() {
        return child.getSchema();
    }

    @Override
    public Tuple next() throws Exception {
        while (true) {
            Tuple tuple = child.next();
            if (tuple == null) {
                return null;
            }
            if (ExpressionEvaluator.evaluate(predicate, tuple)) {
                return tuple;
            }
        }
    }

    @Override
    public void close() throws Exception {
        child.close();
    }
}
