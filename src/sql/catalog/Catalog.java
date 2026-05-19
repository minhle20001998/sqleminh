package sql.catalog;

import sql.buffer.BufferPool;
import sql.page.Page;
import sql.page.PageType;
import sql.schema.Schema;
import sql.table.TableHeap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Catalog {
    private static final int CATALOG_PAGE_ID = 0;
    private static final int TABLE_PAGE_RANGE_SIZE = 1000;
    private static final int FIRST_TABLE_PAGE_ID = 1;

    private final BufferPool bufferPool;
    private final Map<String, TableMetadata> tables;
    private final Map<String, TableHeap> tableHeaps;

    private int nextTableStartPageId;

    public Catalog(BufferPool bufferPool) throws IOException {
        this.bufferPool = bufferPool;
        this.tables = new LinkedHashMap<>();
        this.tableHeaps = new LinkedHashMap<>();
        this.nextTableStartPageId = FIRST_TABLE_PAGE_ID;

        loadCatalog();
    }

    public TableMetadata createTable(String tableName) throws IOException {
        return createTable(tableName, Schema.empty());
    }

    public TableMetadata createTable(String tableName, Schema schema) throws IOException {
        String normalizedName = normalizeTableName(tableName);

        if (tables.containsKey(normalizedName)) {
            throw new IllegalArgumentException("Table already exists: " + tableName);
        }

        int firstPageId = nextTableStartPageId;
        int maxPageId = firstPageId + TABLE_PAGE_RANGE_SIZE - 1;
        nextTableStartPageId += TABLE_PAGE_RANGE_SIZE;

        TableMetadata metadata = new TableMetadata(normalizedName, schema, firstPageId, firstPageId, maxPageId);
        TableHeap tableHeap = new TableHeap(bufferPool, firstPageId, firstPageId, maxPageId);

        tables.put(normalizedName, metadata);
        tableHeaps.put(normalizedName, tableHeap);

        persistCatalog();

        return metadata;
    }

    public boolean tableExists(String tableName) {
        return tables.containsKey(normalizeTableName(tableName));
    }

    public TableMetadata getTableMetadata(String tableName) {
        String normalizedName = normalizeTableName(tableName);
        TableMetadata metadata = tables.get(normalizedName);

        if (metadata == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }

        TableHeap tableHeap = tableHeaps.get(normalizedName);
        metadata.setLastPageId(tableHeap.getLastPageId());

        return metadata;
    }

    public TableHeap getTableHeap(String tableName) {
        String normalizedName = normalizeTableName(tableName);
        TableHeap tableHeap = tableHeaps.get(normalizedName);

        if (tableHeap == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }

        return tableHeap;
    }

    public Collection<TableMetadata> listTables() {
        syncTableLastPageIds();

        return Collections.unmodifiableCollection(tables.values());
    }

    public void flush() throws IOException {
        syncTableLastPageIds();
        persistCatalog();
    }

    private void loadCatalog() throws IOException {
        Page catalogPage = bufferPool.fetchPage(CATALOG_PAGE_ID);

        if (catalogPage.getPageType() != PageType.META || catalogPage.getSlotCount() == 0) {
            catalogPage.initEmpty(CATALOG_PAGE_ID, PageType.META);
            bufferPool.unpinPage(CATALOG_PAGE_ID, true);
            persistCatalog();
            return;
        }

        byte[] catalogBytes = catalogPage.readRecord(0);
        bufferPool.unpinPage(CATALOG_PAGE_ID, false);

        if (catalogBytes == null || catalogBytes.length == 0) {
            return;
        }

        String catalogText = new String(catalogBytes, StandardCharsets.UTF_8);
        String[] lines = catalogText.split("\\R");

        if (lines.length == 0 || lines[0].isBlank()) {
            return;
        }

        nextTableStartPageId = Integer.parseInt(lines[0]);

        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isBlank()) {
                continue;
            }

            String[] parts = lines[i].split("\\|", -1);
            if (parts.length != 4 && parts.length != 5) {
                throw new IllegalStateException("Invalid catalog table entry: " + lines[i]);
            }

            String tableName = normalizeTableName(parts[0]);
            int firstPageId = Integer.parseInt(parts[1]);
            int lastPageId = Integer.parseInt(parts[2]);
            int maxPageId = Integer.parseInt(parts[3]);
            Schema schema = parts.length == 5 ? Schema.deserialize(parts[4]) : Schema.empty();

            TableMetadata metadata = new TableMetadata(tableName, schema, firstPageId, lastPageId, maxPageId);
            TableHeap tableHeap = new TableHeap(bufferPool, firstPageId, lastPageId, maxPageId);

            tables.put(tableName, metadata);
            tableHeaps.put(tableName, tableHeap);
        }
    }

    private void persistCatalog() throws IOException {
        byte[] catalogBytes = serializeCatalog().getBytes(StandardCharsets.UTF_8);

        if (catalogBytes.length + Page.SLOT_SIZE > Page.PAGE_SIZE - Page.HEADER_SIZE) {
            throw new IllegalStateException("Catalog metadata is too large for one page");
        }

        Page catalogPage = bufferPool.fetchPage(CATALOG_PAGE_ID);
        catalogPage.initEmpty(CATALOG_PAGE_ID, PageType.META);

        catalogPage.insertRecord(catalogBytes);
        bufferPool.unpinPage(CATALOG_PAGE_ID, true);
    }

    private String serializeCatalog() {
        StringBuilder builder = new StringBuilder();
        builder.append(nextTableStartPageId).append('\n');

        for (TableMetadata metadata : tables.values()) {
            builder
                    .append(metadata.getTableName()).append('|')
                    .append(metadata.getFirstPageId()).append('|')
                    .append(metadata.getLastPageId()).append('|')
                    .append(metadata.getMaxPageId()).append('|')
                    .append(metadata.getSchema().serialize()).append('\n');
        }

        return builder.toString();
    }

    private void syncTableLastPageIds() {
        for (Map.Entry<String, TableMetadata> entry : tables.entrySet()) {
            TableHeap tableHeap = tableHeaps.get(entry.getKey());
            entry.getValue().setLastPageId(tableHeap.getLastPageId());
        }
    }

    private String normalizeTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        String normalized = tableName.trim().toLowerCase();

        if (normalized.contains("|") || normalized.contains("\n") || normalized.contains("\r")) {
            throw new IllegalArgumentException("Table name contains invalid characters: " + tableName);
        }

        return normalized;
    }
}
