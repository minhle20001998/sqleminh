package sql.parser;

import sql.schema.Schema;

public class CreateTableStatement implements SqlStatement {
    private final String tableName;
    private final Schema schema;

    public CreateTableStatement(String tableName, Schema schema) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }

        this.tableName = tableName.trim().toLowerCase();
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return "CreateTableStatement{" +
                "tableName='" + tableName + '\'' +
                ", schema=" + schema +
                '}';
    }
}
