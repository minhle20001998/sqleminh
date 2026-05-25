package sql.parser;

public class LiteralValue {
    public enum Kind {
        INT,
        TEXT,
        NULL
    }

    private final Kind kind;
    private final Object value;

    private LiteralValue(Kind kind, Object value) {
        this.kind = kind;
        this.value = value;
    }

    public static LiteralValue intLiteral(int value) {
        return new LiteralValue(Kind.INT, value);
    }

    public static LiteralValue textLiteral(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Text literal cannot be null");
        }

        return new LiteralValue(Kind.TEXT, value);
    }

    public static LiteralValue nullLiteral() {
        return new LiteralValue(Kind.NULL, null);
    }

    public Kind getKind() {
        return kind;
    }

    public Object getValue() {
        return value;
    }

    public int asInt() {
        if (kind != Kind.INT) {
            throw new IllegalStateException("Literal is not INT: " + kind);
        }

        return (Integer) value;
    }

    public String asText() {
        if (kind != Kind.TEXT) {
            throw new IllegalStateException("Literal is not TEXT: " + kind);
        }

        return (String) value;
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    @Override
    public String toString() {
        if (kind == Kind.NULL) {
            return "NULL";
        }

        return String.valueOf(value);
    }
}
