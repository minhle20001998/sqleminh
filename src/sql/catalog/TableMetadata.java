package sql.catalog;

public class TableMetadata {
    private final String tableName;
    private final int firstPageId;
    private int lastPageId;
    private final int maxPageId;

    public TableMetadata(String tableName, int firstPageId, int lastPageId) {
        this(tableName, firstPageId, lastPageId, lastPageId);
    }

    public TableMetadata(String tableName, int firstPageId, int lastPageId, int maxPageId) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        if (lastPageId < firstPageId) {
            throw new IllegalArgumentException("Last page id cannot be smaller than first page id");
        }

        if (maxPageId < lastPageId) {
            throw new IllegalArgumentException("Max page id cannot be smaller than last page id");
        }

        this.tableName = tableName;
        this.firstPageId = firstPageId;
        this.lastPageId = lastPageId;
        this.maxPageId = maxPageId;
    }

    public String getTableName() {
        return tableName;
    }

    public int getFirstPageId() {
        return firstPageId;
    }

    public int getLastPageId() {
        return lastPageId;
    }

    public int getMaxPageId() {
        return maxPageId;
    }

    public void setLastPageId(int lastPageId) {
        if (lastPageId < firstPageId) {
            throw new IllegalArgumentException("Last page id cannot be smaller than first page id");
        }

        if (lastPageId > maxPageId) {
            throw new IllegalArgumentException("Last page id cannot be greater than max page id");
        }

        this.lastPageId = lastPageId;
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "tableName='" + tableName + '\'' +
                ", firstPageId=" + firstPageId +
                ", lastPageId=" + lastPageId +
                ", maxPageId=" + maxPageId +
                '}';
    }
}
