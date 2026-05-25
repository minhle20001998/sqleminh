package sql.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InsertStatement implements SqlStatement {
    private final String tableName;
    private final List<String> columnNames;
    private final List<LiteralValue> values;

    public InsertStatement(String tableName, List<String> columnNames, List<LiteralValue> values) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        if (columnNames == null) {
            throw new IllegalArgumentException("Column names cannot be null");
        }

        if (values == null) {
            throw new IllegalArgumentException("Values cannot be null");
        }

        this.tableName = tableName.trim().toLowerCase();
        this.columnNames = normalizeColumnNames(columnNames);
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<LiteralValue> getValues() {
        return values;
    }

    public boolean hasColumnList() {
        return !columnNames.isEmpty();
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
        return "InsertStatement{" +
                "tableName='" + tableName + '\'' +
                ", columnNames=" + columnNames +
                ", values=" + values +
                '}';
    }
}
