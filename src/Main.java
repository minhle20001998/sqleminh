import sql.buffer.BufferPool;
import sql.buffer.Frame;
import sql.page.Page;
import sql.page.PageType;
import sql.page.Slot;
import sql.storage.DiskManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {
//        testPage();
        testBuffer();

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