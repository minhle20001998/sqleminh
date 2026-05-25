package sql.binder;

import sql.catalog.Catalog;
import sql.catalog.TableMetadata;
import sql.parser.ComparisonExpression;
import sql.parser.CreateTableStatement;
import sql.parser.InsertStatement;
import sql.parser.LiteralValue;
import sql.parser.SelectStatement;
import sql.parser.SqlStatement;
import sql.schema.Column;
import sql.schema.Schema;
import sql.schema.SqlType;
import sql.tuple.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlBinder {
    private final Catalog catalog;

    public SqlBinder(Catalog catalog) {
        this.catalog = catalog;
    }

    public BoundStatement bind(SqlStatement statement) {
        if (statement instanceof CreateTableStatement createTableStatement) {
            return bindCreateTable(createTableStatement);
        }

        if (statement instanceof InsertStatement insertStatement) {
            return bindInsert(insertStatement);
        }

        if (statement instanceof SelectStatement selectStatement) {
            return bindSelect(selectStatement);
        }

        throw new IllegalArgumentException("Unsupported SQL statement: " + statement.getClass().getSimpleName());
    }

    private BoundCreateTableStatement bindCreateTable(CreateTableStatement statement) {
        if (catalog.tableExists(statement.getTableName())) {
            throw new IllegalArgumentException("Table already exists: " + statement.getTableName());
        }

        if (statement.getSchema().isEmpty()) {
            throw new IllegalArgumentException("CREATE TABLE requires at least one column");
        }

        return new BoundCreateTableStatement(statement.getTableName(), statement.getSchema());
    }

    private BoundInsertStatement bindInsert(InsertStatement statement) {
        TableMetadata tableMetadata = catalog.getTableMetadata(statement.getTableName());
        Schema schema = tableMetadata.getSchema();

        if (schema.isEmpty()) {
            throw new IllegalArgumentException("Cannot insert into table without schema: " + statement.getTableName());
        }

        List<Value> valuesInSchemaOrder = statement.hasColumnList()
                ? bindInsertWithColumnList(statement, schema)
                : bindInsertWithoutColumnList(statement, schema);

        return new BoundInsertStatement(tableMetadata, valuesInSchemaOrder);
    }

    private List<Value> bindInsertWithoutColumnList(InsertStatement statement, Schema schema) {
        if (statement.getValues().size() != schema.size()) {
            throw new IllegalArgumentException(
                    "INSERT value count does not match table schema. expected=" + schema.size() +
                            ", actual=" + statement.getValues().size()
            );
        }

        List<Value> values = new ArrayList<>();

        for (int i = 0; i < schema.size(); i++) {
            values.add(convertLiteral(statement.getValues().get(i), schema.getColumn(i)));
        }

        return values;
    }

    private List<Value> bindInsertWithColumnList(InsertStatement statement, Schema schema) {
        if (statement.getColumnNames().size() != statement.getValues().size()) {
            throw new IllegalArgumentException(
                    "INSERT column count does not match value count. columns=" +
                            statement.getColumnNames().size() +
                            ", values=" + statement.getValues().size()
            );
        }

        List<Value> values = new ArrayList<>();
        for (int i = 0; i < schema.size(); i++) {
            values.add(null);
        }

        Set<String> seenColumnNames = new HashSet<>();

        for (int i = 0; i < statement.getColumnNames().size(); i++) {
            String columnName = statement.getColumnNames().get(i);

            if (!seenColumnNames.add(columnName)) {
                throw new IllegalArgumentException("Duplicate INSERT column: " + columnName);
            }

            int columnIndex = schema.getColumnIndex(columnName);
            Column column = schema.getColumn(columnIndex);
            values.set(columnIndex, convertLiteral(statement.getValues().get(i), column));
        }

        for (int i = 0; i < schema.size(); i++) {
            Column column = schema.getColumn(i);

            if (values.get(i) == null) {
                if (!column.isNullable()) {
                    throw new IllegalArgumentException("Missing required column: " + column.getName());
                }

                values.set(i, Value.nullValue(column.getType()));
            }
        }

        return values;
    }

    private BoundSelectStatement bindSelect(SelectStatement statement) {
        TableMetadata tableMetadata = catalog.getTableMetadata(statement.getTableName());
        Schema schema = tableMetadata.getSchema();
        List<Integer> selectedIndexes = new ArrayList<>();
        List<String> selectedNames = new ArrayList<>();

        if (statement.isSelectAll()) {
            for (int i = 0; i < schema.size(); i++) {
                selectedIndexes.add(i);
                selectedNames.add(schema.getColumn(i).getName());
            }
        } else {
            Set<String> seenColumnNames = new HashSet<>();

            for (String columnName : statement.getColumnNames()) {
                if (!seenColumnNames.add(columnName)) {
                    throw new IllegalArgumentException("Duplicate SELECT column: " + columnName);
                }

                int columnIndex = schema.getColumnIndex(columnName);
                selectedIndexes.add(columnIndex);
                selectedNames.add(schema.getColumn(columnIndex).getName());
            }
        }

        BoundComparisonExpression where = null;

        if (statement.hasWhere()) {
            where = bindWhere(statement.getWhere(), schema);
        }

        return new BoundSelectStatement(
                tableMetadata,
                statement.isSelectAll(),
                selectedIndexes,
                selectedNames,
                where
        );
    }

    private BoundComparisonExpression bindWhere(ComparisonExpression expression, Schema schema) {
        int columnIndex = schema.getColumnIndex(expression.getColumnName());
        Column column = schema.getColumn(columnIndex);
        Value value = convertLiteral(expression.getLiteral(), column);

        return new BoundComparisonExpression(
                columnIndex,
                column.getName(),
                expression.getOperator(),
                value
        );
    }

    private Value convertLiteral(LiteralValue literal, Column column) {
        if (literal.isNull()) {
            if (!column.isNullable()) {
                throw new IllegalArgumentException("Column cannot be NULL: " + column.getName());
            }

            return Value.nullValue(column.getType());
        }

        if (column.getType() == SqlType.INT && literal.getKind() == LiteralValue.Kind.INT) {
            return Value.intValue(literal.asInt());
        }

        if (column.getType() == SqlType.TEXT && literal.getKind() == LiteralValue.Kind.TEXT) {
            return Value.textValue(literal.asText());
        }

        throw new IllegalArgumentException(
                "Literal type does not match column " + column.getName() +
                        ". expected=" + column.getType() +
                        ", actual=" + literal.getKind()
        );
    }
}
