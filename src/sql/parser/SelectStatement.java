package sql.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SelectStatement implements SqlStatement {
    private final boolean selectAll;
    private final List<String> columnNames;
    private final String tableName;
    private final ComparisonExpression where;

    public SelectStatement(boolean selectAll, List<String> columnNames, String tableName, ComparisonExpression where) {
        if (!selectAll && (columnNames == null || columnNames.isEmpty())) {
            throw new IllegalArgumentException("Select column list cannot be empty");
        }

        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        this.selectAll = selectAll;
        this.columnNames = normalizeColumnNames(columnNames == null ? Collections.emptyList() : columnNames);
        this.tableName = tableName.trim().toLowerCase();
        this.where = where;
    }

    public boolean isSelectAll() {
        return selectAll;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getTableName() {
        return tableName;
    }

    public ComparisonExpression getWhere() {
        return where;
    }

    public boolean hasWhere() {
        return where != null;
    }

    private List<String> normalizeColumnNames(List<String> columnNames) {
        List<String> normalizedNames = new ArrayList<>();

        for (String columnName : columnNames) {
            if (columnName == null || columnName.isBlank()) {
                throw new IllegalArgumentException("Column name cannot be empty");
            }

            normalizedNames.add(columnName.trim().toLowerCase());
        }

        return Collections.unmodifiableList(normalizedNames);
    }

    @Override
    public String toString() {
        return "SelectStatement{" +
                "selectAll=" + selectAll +
                ", columnNames=" + columnNames +
                ", tableName='" + tableName + '\'' +
                ", where=" + where +
                '}';
    }
}
