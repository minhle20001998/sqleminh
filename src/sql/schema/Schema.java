package sql.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Schema {
    private final List<Column> columns;

    public Schema(List<Column> columns) {
        if (columns == null) {
            throw new IllegalArgumentException("Columns cannot be null");
        }

        Set<String> seenNames = new HashSet<>();
        List<Column> copiedColumns = new ArrayList<>();

        for (Column column : columns) {
            if (column == null) {
                throw new IllegalArgumentException("Column cannot be null");
            }

            if (!seenNames.add(column.getName())) {
                throw new IllegalArgumentException("Duplicate column name: " + column.getName());
            }

            copiedColumns.add(column);
        }

        this.columns = Collections.unmodifiableList(copiedColumns);
    }

    public static Schema empty() {
        return new Schema(List.of());
    }

    public int size() {
        return columns.size();
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public Column getColumn(String name) {
        int index = getColumnIndex(name);
        return columns.get(index);
    }

    public int getColumnIndex(String name) {
        String normalizedName = normalizeColumnName(name);

        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(normalizedName)) {
                return i;
            }
        }

        throw new IllegalArgumentException("Unknown column: " + name);
    }

    public boolean hasColumn(String name) {
        String normalizedName = normalizeColumnName(name);

        for (Column column : columns) {
            if (column.getName().equals(normalizedName)) {
                return true;
            }
        }

        return false;
    }

    public String serialize() {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            if (i > 0) {
                builder.append(',');
            }

            builder
                    .append(column.getName()).append(':')
                    .append(column.getType().name()).append(':')
                    .append(column.isNullable());
        }

        return builder.toString();
    }

    public static Schema deserialize(String value) {
        if (value == null || value.isBlank()) {
            return Schema.empty();
        }

        String[] columnParts = value.split(",");
        List<Column> columns = new ArrayList<>();

        for (String columnPart : columnParts) {
            String[] parts = columnPart.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid schema column: " + columnPart);
            }

            columns.add(new Column(
                    parts[0],
                    SqlType.fromName(parts[1]),
                    Boolean.parseBoolean(parts[2])
            ));
        }

        return new Schema(columns);
    }

    private String normalizeColumnName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be empty");
        }

        return name.trim().toLowerCase();
    }

    @Override
    public String toString() {
        return "Schema{" +
                "columns=" + columns +
                '}';
    }
}
