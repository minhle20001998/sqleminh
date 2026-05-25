package sql.parser;

public class ComparisonExpression {
    private final String columnName;
    private final ComparisonOperator operator;
    private final LiteralValue literal;

    public ComparisonExpression(String columnName, ComparisonOperator operator, LiteralValue literal) {
        if (columnName == null || columnName.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be empty");
        }

        if (operator == null) {
            throw new IllegalArgumentException("Comparison operator cannot be null");
        }

        if (literal == null) {
            throw new IllegalArgumentException("Comparison literal cannot be null");
        }

        this.columnName = columnName.trim().toLowerCase();
        this.operator = operator;
        this.literal = literal;
    }

    public String getColumnName() {
        return columnName;
    }

    public ComparisonOperator getOperator() {
        return operator;
    }

    public LiteralValue getLiteral() {
        return literal;
    }

    @Override
    public String toString() {
        return "ComparisonExpression{" +
                "columnName='" + columnName + '\'' +
                ", operator=" + operator +
                ", literal=" + literal +
                '}';
    }
}
