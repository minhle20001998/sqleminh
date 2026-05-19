package sql.schema;

import java.util.Objects;

public class Column {
    private final String name;
    private final SqlType type;
    private final boolean nullable;

    public Column(String name, SqlType type) {
        this(name, type, true);
    }

    public Column(String name, SqlType type, boolean nullable) {
        this.name = normalizeName(name);
        this.type = Objects.requireNonNull(type, "Column type cannot be null");
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public SqlType getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    private String normalizeName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be empty");
        }

        String normalized = name.trim().toLowerCase();

        if (normalized.contains("|") || normalized.contains(",") || normalized.contains(":")
                || normalized.contains("\n") || normalized.contains("\r")) {
            throw new IllegalArgumentException("Column name contains invalid characters: " + name);
        }

        return normalized;
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", nullable=" + nullable +
                '}';
    }
}
