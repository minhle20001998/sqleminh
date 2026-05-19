import sql.buffer.BufferPool;
import sql.buffer.Frame;
import sql.catalog.Catalog;
import sql.catalog.TableMetadata;
import sql.page.Page;
import sql.page.PageType;
import sql.page.Slot;
import sql.record.RecordId;
import sql.schema.Column;
import sql.schema.Schema;
import sql.schema.SqlType;
import sql.storage.DiskManager;
import sql.table.SequentialScan;
import sql.table.TableHeap;
import sql.tuple.Tuple;
import sql.tuple.Value;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws Exception {
//        testPage();
//        testBuffer();
//        testTableHeap();
//        testSequentialScan();
//        testDeleteAndScan();
//        testUpdate();
//        testCatalog();
        testTuple();
    }

    private static void testPage() throws IOException {
        // Create a new page
        Page page = new Page(0, PageType.DATA);

        // Debug info (before)
        System.out.println("\n--- Page Debug ---");
        System.out.println("Slot count: " + page.getSlotCount());
        System.out.println("Free space left: " + page.getFreeSpaceLeft());
        System.out.println("------\n");

        // Insert some records
        int slot1 = page.insertRecord("Hello".getBytes(StandardCharsets.UTF_8));
        int slot2 = page.insertRecord("World".getBytes(StandardCharsets.UTF_8));
        int slot3 = page.insertRecord("This is a database page".getBytes(StandardCharsets.UTF_8));

        System.out.println("Inserted slots: " + slot1 + ", " + slot2 + ", " + slot3);

        // Read records back
        printRecord(page, slot1);
        printRecord(page, slot2);
        printRecord(page, slot3);

        // Delete a record
        page.deleteRecord(slot2);
        System.out.println("\nDeleted slot " + slot2);
        byte[] deleted = page.readRecord(slot2);
        System.out.println("Read deleted slot: " + Arrays.toString(deleted));

        // Insert another record (tests reuse of space)
        int slot4 = page.insertRecord("New shii".getBytes(StandardCharsets.UTF_8));
        System.out.println("\nInserted slot: " + slot4);
        printRecord(page, slot4);

        // Debug info (after)
        System.out.println("\n--- Page Debug ---");
        System.out.println("Slot count: " + page.getSlotCount());
        System.out.println("Free space left: " + page.getFreeSpaceLeft());
        System.out.println("------\n");

        // Test disk
        DiskManager diskManager = new DiskManager("test.db", Page.PAGE_SIZE);
        // create page
        Page page1 = new Page(0, PageType.DATA);
        page1.insertRecord("save this".getBytes());
        page1.insertRecord("save that".getBytes());

        // write to disk
        diskManager.writePage(0, page1.getData());
        // read back into a new page object
        byte[] pageBytes = new byte[Page.PAGE_SIZE];
        diskManager.readPage(0, pageBytes);

        Page page2 = new Page(pageBytes);

        // verify
        System.out.println("Page 1 content read from disk: " + new String(page2.readRecord(0)));
        System.out.println("Page 2 content read from disk: " + new String(page2.readRecord(1)));

        diskManager.close();
    }

    private static void testBuffer() throws IOException {
        Path dbFile = Path.of("test.db");
        Files.deleteIfExists(dbFile);

        DiskManager diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        BufferPool bufferPool = new BufferPool(2, diskManager);

        System.out.println("=== Test 1: Fetch & modify page ===");
        Page page1 = bufferPool.fetchPage(1);
        page1.insertRecord("Hello".getBytes());
        bufferPool.unpinPage(1, true); // mark dirty
        System.out.println("Inserted record into page 1");

        System.out.println("\n=== Test 2: Fetch same page again (should be cached) ===");
        Page page1Again = bufferPool.fetchPage(1);
        byte[] data = page1Again.readRecord(0);
        System.out.println("Read from page 1: " + new String(data));
        bufferPool.unpinPage(1, false);

        System.out.println("\n=== Test 3: Fill buffer pool & trigger eviction ===");
        Page page2 = bufferPool.fetchPage(2);
        page2.insertRecord("World".getBytes());
        bufferPool.unpinPage(2, true);
        System.out.println("Page 2 inserted");
        // Buffer pool max = 2
        // Fetching page 3 must evict page 1 or 2
        Page page3 = bufferPool.fetchPage(3);
        page3.insertRecord("Eviction test".getBytes());
        bufferPool.unpinPage(3, true);
        System.out.println("Page 3 inserted (eviction happened)");
        for (var entry : bufferPool.getPageTable().entrySet()) {
            int pageId = entry.getKey();
            Frame frame = entry.getValue();
            Page page = frame.getPage();

            System.out.println(
                    "PageId=" + pageId +
                            " | pin=" + frame.getPinCount() +
                            " | dirty=" + frame.isDirty() +
                            " | slots=" + page.getSlotCount() +
                            " | free=" + page.getFreeSpace()
            );
        }

        System.out.println("\n=== Test 4: Flush all pages ===");
        bufferPool.flushAll();
        System.out.println("All dirty pages flushed");
        System.out.println("\n--- Buffer Pool State ---");

        for (var entry : bufferPool.getPageTable().entrySet()) {
            int pageId = entry.getKey();
            Frame frame = entry.getValue();
            Page page = frame.getPage();

            System.out.println(
                    "PageId=" + pageId +
                            " | pin=" + frame.getPinCount() +
                            " | dirty=" + frame.isDirty() +
                            " | slots=" + page.getSlotCount() +
                            " | free=" + page.getFreeSpace()
            );
        }

        System.out.println("-------------------------\n");
    }

    private static void testTableHeap() throws IOException {
        Path dbFile = Path.of("tableheap_test.db");
        Files.deleteIfExists(dbFile);

        DiskManager diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        BufferPool bufferPool = new BufferPool(2, diskManager);

        System.out.println("=== Create TableHeap ===");
        TableHeap table = new TableHeap(bufferPool, 0);

        System.out.println("=== Insert records ===");
        RecordId r1 = table.insert("Hello".getBytes());
        RecordId r2 = table.insert("World".getBytes());
        RecordId r3 = table.insert("Database".getBytes());

        System.out.println("\n=== Read records ===");

        System.out.println("\n=== Verify page state ===");
        Page page = bufferPool.fetchPage(r1.getPageId());

        System.out.println("PageId: " + page.getPageId());
        System.out.println("SlotCount: " + page.getSlotCount());

        // Expect 3
        assert page.getSlotCount() == 3 : "Slot count should be 3";

        bufferPool.unpinPage(page.getPageId(), false);

        System.out.println("\n=== Flush & Reload ===");
        bufferPool.flushAll();

        // Reopen DB
        diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        bufferPool = new BufferPool(2, diskManager);
        table = new TableHeap(bufferPool, 0);

        System.out.println(new String(table.read(r1)));
        System.out.println(new String(table.read(r2)));
        System.out.println(new String(table.read(r3)));

        System.out.println("\nTableHeap basic test PASSED");
    }

    private static void testSequentialScan() throws Exception {
        System.out.println("\n=== SequentialScan Test ===");

        Path dbFile = Path.of("scan_test.db");
        Files.deleteIfExists(dbFile);

        DiskManager diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        BufferPool bufferPool = new BufferPool(2, diskManager);
        TableHeap table = new TableHeap(bufferPool, 0);

        // Insert records
        table.insert("A".getBytes());
        table.insert("B".getBytes());
        table.insert("C".getBytes());
        table.insert("D".getBytes());

        System.out.println("Inserted 4 records");

        // Scan
        SequentialScan scan = new SequentialScan(bufferPool, 0, table.getLastPageId());

        int count = 0;
        byte[] record;

        while ((record = scan.next()) != null) {
            System.out.println("Scan -> " + new String(record));
            count++;
        }

        scan.close();

        // Verify
        if (count != 4) {
            throw new IllegalStateException("Expected 4 records, got " + count);
        }

        System.out.println("SequentialScan PASSED");
    }

    private static void testDeleteAndScan() throws Exception {
        System.out.println("\n=== Delete + SequentialScan Test ===");

        Path dbFile = Path.of("delete_scan_test.db");
        Files.deleteIfExists(dbFile);

        DiskManager diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        BufferPool bufferPool = new BufferPool(2, diskManager);
        TableHeap table = new TableHeap(bufferPool, 0);

        RecordId r1 = table.insert("A".getBytes());
        RecordId r2 = table.insert("B".getBytes());
        RecordId r3 = table.insert("C".getBytes());

        System.out.println("Inserted A, B, C");

        // Delete B
        table.delete(r2);
        System.out.println("Deleted B");

        SequentialScan scan =
                new SequentialScan(bufferPool, 0, table.getLastPageId());

        int count = 0;
        byte[] record;

        while ((record = scan.next()) != null) {
            System.out.println("Scan -> " + new String(record));
            count++;
        }

        scan.close();

        // Expect only A and C
        if (count != 2) {
            throw new IllegalStateException(
                    "Expected 2 records after delete, got " + count
            );
        }

        System.out.println("Delete + SequentialScan PASSED");
    }

    private static void testUpdate() throws Exception {
        System.out.println("\n=== UPDATE Test ===");

        Path dbFile = Path.of("update_test.db");
        Files.deleteIfExists(dbFile);

        DiskManager diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        BufferPool bufferPool = new BufferPool(2, diskManager);
        TableHeap table = new TableHeap(bufferPool, 0);

        // Insert
        RecordId r1 = table.insert("Hello".getBytes());

        // Case 1: overwrite in place
        RecordId r1b = table.update(r1, "Hi".getBytes());
        byte[] a = table.read(r1b);
        System.out.println("Update in place a -> " + new String(a));
        Page p1 = bufferPool.fetchPage(r1b.getPageId());
        bufferPool.unpinPage(r1b.getPageId(), false);
        System.out.println("Data 1: " + Arrays.toString(p1.getData()));

        // Case 2: force move
        RecordId r2 = table.update(r1b, "Hello World!!!".getBytes());
        byte[] b = table.read(r2);
        System.out.println("Update in place b -> " + new String(b));
        Page p2 = bufferPool.fetchPage(r2.getPageId());
        bufferPool.unpinPage(r2.getPageId(), false);
        System.out.println("Data 1: " + Arrays.toString(p1.getData()));

        System.out.println("UPDATE PASSED");
    }

    private static void testCatalog() throws Exception {
        System.out.println("\n=== Catalog Test ===");

        Path dbFile = Path.of("catalog_test.db");
        Files.deleteIfExists(dbFile);

        DiskManager diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        BufferPool bufferPool = new BufferPool(3, diskManager);
        Catalog catalog = new Catalog(bufferPool);

        Schema usersSchema = new Schema(Arrays.asList(
                new Column("id", SqlType.INT, false),
                new Column("name", SqlType.TEXT, true)
        ));
        Schema postsSchema = new Schema(Arrays.asList(
                new Column("id", SqlType.INT, false),
                new Column("title", SqlType.TEXT, false)
        ));

        TableMetadata usersMetadata = catalog.createTable("users", usersSchema);
        TableMetadata postsMetadata = catalog.createTable("posts", postsSchema);

        System.out.println("Created table: " + usersMetadata);
        System.out.println("Created table: " + postsMetadata);

        TableHeap users = catalog.getTableHeap("users");
        TableHeap posts = catalog.getTableHeap("posts");

        RecordId userRid = users.insert("user:1:minh".getBytes());
        RecordId postRid = posts.insert("post:1:hello".getBytes());

        System.out.println("User row -> " + new String(users.read(userRid)));
        System.out.println("Post row -> " + new String(posts.read(postRid)));

        System.out.println("\nCatalog tables:");
        for (TableMetadata metadata : catalog.listTables()) {
            System.out.println(metadata);
        }

        catalog.flush();
        bufferPool.flushAll();
        diskManager.close();

        System.out.println("\n=== Reload Catalog Test ===");

        diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        bufferPool = new BufferPool(3, diskManager);
        catalog = new Catalog(bufferPool);

        System.out.println("Loaded users metadata: " + catalog.getTableMetadata("users"));
        System.out.println("Loaded posts metadata: " + catalog.getTableMetadata("posts"));
        System.out.println("Loaded users name column: " + catalog.getTableMetadata("users").getSchema().getColumn("name"));

        users = catalog.getTableHeap("users");
        posts = catalog.getTableHeap("posts");

        System.out.println("Reloaded user row -> " + new String(users.read(userRid)));
        System.out.println("Reloaded post row -> " + new String(posts.read(postRid)));

        TableHeap limitedTable = new TableHeap(bufferPool, 3001, 3001, 3001);
        byte[] largeRecord = new byte[Page.PAGE_SIZE - Page.HEADER_SIZE - Page.SLOT_SIZE];
        Arrays.fill(largeRecord, (byte) 'x');
        limitedTable.insert(largeRecord);

        try {
            limitedTable.insert("overflow".getBytes());
            throw new IllegalStateException("Expected table page range overflow");
        } catch (IllegalStateException expected) {
            System.out.println("Range limit check -> " + expected.getMessage());
        }

        diskManager.close();

        System.out.println("Catalog persistence test PASSED");
    }

    private static void testTuple() throws Exception {
        System.out.println("\n=== Tuple Test ===");

        Path dbFile = Path.of("tuple_test.db");
        Files.deleteIfExists(dbFile);

        DiskManager diskManager = new DiskManager(dbFile.toString(), Page.PAGE_SIZE);
        BufferPool bufferPool = new BufferPool(3, diskManager);
        Catalog catalog = new Catalog(bufferPool);

        Schema usersSchema = new Schema(Arrays.asList(
                new Column("id", SqlType.INT, false),
                new Column("name", SqlType.TEXT, true)
        ));

        catalog.createTable("users", usersSchema);
        TableHeap users = catalog.getTableHeap("users");

        Tuple tuple = new Tuple(usersSchema, Arrays.asList(
                Value.intValue(1),
                Value.textValue("Minh")
        ));

        RecordId rid = users.insert(tuple.serialize());
        Tuple loaded = Tuple.deserialize(usersSchema, users.read(rid));

        System.out.println("Loaded id -> " + loaded.getValue("id").asInt());
        System.out.println("Loaded name -> " + loaded.getValue("name").asText());

        Tuple nullableTuple = new Tuple(usersSchema, Arrays.asList(
                Value.intValue(2),
                Value.nullValue(SqlType.TEXT)
        ));

        RecordId nullRid = users.insert(nullableTuple.serialize());
        Tuple loadedNull = Tuple.deserialize(usersSchema, users.read(nullRid));

        System.out.println("Loaded nullable id -> " + loadedNull.getValue("id").asInt());
        System.out.println("Loaded nullable name is null -> " + loadedNull.getValue("name").isNull());

        catalog.flush();
        bufferPool.flushAll();
        diskManager.close();

        System.out.println("Tuple test PASSED");
    }

    private static void printRecord(Page page, int slot) {
        byte[] data = page.readRecord(slot);
        if (data == null) {
            System.out.println("Slot " + slot + ": <deleted>");
        } else {
            System.out.println(
                    "Slot " + slot + ": " +
                            new String(data, StandardCharsets.UTF_8) +
                            " (" + data.length + " bytes)"
            );
        }
    }
}
