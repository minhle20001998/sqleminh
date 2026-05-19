package sql.tuple;

import sql.schema.SqlType;

import java.util.Objects;

public class Value {
    private final SqlType type;
    private final Object value;

    private Value(SqlType type, Object value) {
        this.type = Objects.requireNonNull(type, "Value type cannot be null");
        this.value = value;
    }

    public static Value intValue(int value) {
        return new Value(SqlType.INT, value);
    }

    public static Value textValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Use nullValue(type) for SQL NULL values");
        }

        return new Value(SqlType.TEXT, value);
    }

    public static Value nullValue(SqlType type) {
        return new Value(type, null);
    }

    public SqlType getType() {
        return type;
    }

    public boolean isNull() {
        return value == null;
    }

    public int asInt() {
        if (isNull()) {
            throw new IllegalStateException("Cannot read NULL as INT");
        }

        if (type != SqlType.INT) {
            throw new IllegalStateException("Value is not INT: " + type);
        }

        return (Integer) value;
    }

    public String asText() {
        if (isNull()) {
            throw new IllegalStateException("Cannot read NULL as TEXT");
        }

        if (type != SqlType.TEXT) {
            throw new IllegalStateException("Value is not TEXT: " + type);
        }

        return (String) value;
    }

    @Override
    public String toString() {
        if (isNull()) {
            return "NULL";
        }

        return String.valueOf(value);
    }
}
