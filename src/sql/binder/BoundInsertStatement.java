package sql.binder;

import sql.catalog.TableMetadata;
import sql.tuple.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BoundInsertStatement implements BoundStatement {
    private final TableMetadata tableMetadata;
    private final List<Value> valuesInSchemaOrder;

    public BoundInsertStatement(TableMetadata tableMetadata, List<Value> valuesInSchemaOrder) {
        this.tableMetadata = tableMetadata;
        this.valuesInSchemaOrder = Collections.unmodifiableList(new ArrayList<>(valuesInSchemaOrder));
    }

    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    public List<Value> getValuesInSchemaOrder() {
        return valuesInSchemaOrder;
    }

    @Override
    public String toString() {
        return "BoundInsertStatement{" +
                "tableMetadata=" + tableMetadata +
                ", valuesInSchemaOrder=" + valuesInSchemaOrder +
                '}';
    }
}
