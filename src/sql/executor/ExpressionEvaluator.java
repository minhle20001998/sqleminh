package sql.executor;

import sql.binder.BoundComparisonExpression;
import sql.parser.ComparisonOperator;
import sql.tuple.Tuple;
import sql.tuple.Value;

public class ExpressionEvaluator {
    public static boolean evaluate(BoundComparisonExpression expression, Tuple tuple) {
        Value columnValue = tuple.getValue(expression.getColumnIndex());

        if (columnValue.isNull() || expression.getValue().isNull()) {
            return false;
        }

        ComparisonOperator op = expression.getOperator();
        Value rightValue = expression.getValue();

        if (op == ComparisonOperator.EQUALS) {
            return columnValueEquals(columnValue, rightValue);
        }

        throw new IllegalArgumentException("Unsupported comparison operator: " + op);
    }

    private static boolean columnValueEquals(Value left, Value right) {
        switch (left.getType()) {
            case INT:
                return left.asInt() == right.asInt();
            case TEXT:
                return left.asText().equals(right.asText());
            default:
                throw new IllegalArgumentException("Unsupported type for comparison: " + left.getType());
        }
    }
}
