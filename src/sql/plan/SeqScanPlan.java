package sql.plan;

import sql.catalog.TableMetadata;

public class SeqScanPlan implements LogicalPlan {
    private final TableMetadata tableMetadata;

    public SeqScanPlan(TableMetadata tableMetadata) {
        this.tableMetadata = tableMetadata;
    }

    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    @Override
    public String toString() {
        return "SeqScanPlan{" +
                "tableMetadata=" + tableMetadata +
                '}';
    }
}
