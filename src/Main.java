import sql.page.Page;
import sql.page.PageType;
import sql.storage.DiskManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {

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