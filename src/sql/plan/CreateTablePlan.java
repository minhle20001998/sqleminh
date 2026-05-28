package sql.plan;

import sql.schema.Schema;

public class CreateTablePlan implements LogicalPlan {
    private final String tableName;
    private final Schema schema;

    public CreateTablePlan(String tableName, Schema schema) {
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
        return "CreateTablePlan{" +
                "tableName='" + tableName + '\'' +
                ", schema=" + schema +
                '}';
    }
}
