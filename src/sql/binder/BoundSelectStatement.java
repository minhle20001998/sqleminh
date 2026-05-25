package sql.binder;

import sql.catalog.TableMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BoundSelectStatement implements BoundStatement {
    private final TableMetadata tableMetadata;
    private final boolean selectAll;
    private final List<Integer> selectedColumnIndexes;
    private final List<String> selectedColumnNames;
    private final BoundComparisonExpression where;

    public BoundSelectStatement(
            TableMetadata tableMetadata,
            boolean selectAll,
            List<Integer> selectedColumnIndexes,
            List<String> selectedColumnNames,
            BoundComparisonExpression where
    ) {
        this.tableMetadata = tableMetadata;
        this.selectAll = selectAll;
        this.selectedColumnIndexes = Collections.unmodifiableList(new ArrayList<>(selectedColumnIndexes));
        this.selectedColumnNames = Collections.unmodifiableList(new ArrayList<>(selectedColumnNames));
        this.where = where;
    }

    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    public boolean isSelectAll() {
        return selectAll;
    }

    public List<Integer> getSelectedColumnIndexes() {
        return selectedColumnIndexes;
    }

    public List<String> getSelectedColumnNames() {
        return selectedColumnNames;
    }

    public BoundComparisonExpression getWhere() {
        return where;
    }

    public boolean hasWhere() {
        return where != null;
    }

    @Override
    public String toString() {
        return "BoundSelectStatement{" +
                "tableMetadata=" + tableMetadata +
                ", selectAll=" + selectAll +
                ", selectedColumnIndexes=" + selectedColumnIndexes +
                ", selectedColumnNames=" + selectedColumnNames +
                ", where=" + where +
                '}';
    }
}
