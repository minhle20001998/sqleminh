package sql.plan;

import sql.binder.BoundCreateTableStatement;
import sql.binder.BoundInsertStatement;
import sql.binder.BoundSelectStatement;
import sql.binder.BoundStatement;

public class LogicalPlanner {
    public LogicalPlan plan(BoundStatement statement) {
        if (statement instanceof BoundCreateTableStatement createTableStatement) {
            return new CreateTablePlan(
                    createTableStatement.getTableName(),
                    createTableStatement.getSchema()
            );
        }

        if (statement instanceof BoundInsertStatement insertStatement) {
            return new InsertPlan(
                    insertStatement.getTableMetadata(),
                    insertStatement.getValuesInSchemaOrder()
            );
        }

        if (statement instanceof BoundSelectStatement selectStatement) {
            return planSelect(selectStatement);
        }

        throw new IllegalArgumentException("Unsupported bound statement: " + statement.getClass().getSimpleName());
    }

    private LogicalPlan planSelect(BoundSelectStatement statement) {
        LogicalPlan plan = new SeqScanPlan(statement.getTableMetadata());

        if (statement.hasWhere()) {
            plan = new FilterPlan(plan, statement.getWhere());
        }

        if (!statement.isSelectAll()) {
            plan = new ProjectionPlan(
                    plan,
                    statement.getSelectedColumnIndexes(),
                    statement.getSelectedColumnNames()
            );
        }

        return plan;
    }
}
