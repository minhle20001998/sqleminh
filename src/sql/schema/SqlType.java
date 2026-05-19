package sql.schema;

public enum SqlType {
    INT,
    TEXT;

    public static SqlType fromName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("SQL type name cannot be empty");
        }

        return SqlType.valueOf(name.trim().toUpperCase());
    }
}
