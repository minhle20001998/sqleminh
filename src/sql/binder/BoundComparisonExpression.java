package sql.binder;

import sql.parser.ComparisonOperator;
import sql.tuple.Value;

public class BoundComparisonExpression {
    private final int columnIndex;
    private final String columnName;
    private final ComparisonOperator operator;
    private final Value value;

    public BoundComparisonExpression(int columnIndex, String columnName, ComparisonOperator operator, Value value) {
        this.columnIndex = columnIndex;
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getColumnName() {
        return columnName;
    }

    public ComparisonOperator getOperator() {
        return operator;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "BoundComparisonExpression{" +
                "columnIndex=" + columnIndex +
                ", columnName='" + columnName + '\'' +
                ", operator=" + operator +
                ", value=" + value +
                '}';
    }
}
