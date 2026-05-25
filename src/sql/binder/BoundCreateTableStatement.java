package sql.binder;

import sql.schema.Schema;

public class BoundCreateTableStatement implements BoundStatement {
    private final String tableName;
    private final Schema schema;

    public BoundCreateTableStatement(String tableName, Schema schema) {
        this.tableName = tableName;
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
        return "BoundCreateTableStatement{" +
                "tableName='" + tableName + '\'' +
                ", schema=" + schema +
                '}';
    }
}
