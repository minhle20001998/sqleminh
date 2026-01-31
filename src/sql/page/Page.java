package sql.page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Page {
    // Page constants
    public static final int PAGE_SIZE = 4096;
    public static final int HEADER_SIZE = 16;
    public static final int SLOT_SIZE = 4;

    // Header byte offsets
    private static final int PAGE_ID_OFFSET = 0;
    private static final int PAGE_TYPE_OFFSET = 4;
    private static final int FLAGS_OFFSET = 5;
    private static final int SLOT_COUNT_OFFSET = 6;
    private static final int FREE_SPACE_OFFSET_OFFSET = 8;
    private static final int FREE_SPACE_SIZE_OFFSET = 10;
    private static final int CHECKSUM_OFFSET = 12;

    // Raw page storage
    private final byte[] data;
    private final ByteBuffer buffer;


    // Constructor
    //  - Empty page
    public Page(int pageId, PageType type) {
        this.data = new byte[PAGE_SIZE];
        // ByteBuffer provides byte manipulation
        this.buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        initHeader(pageId, type);
    }

    // - Read from disk
    public Page(byte[] data) {
        if (data.length != PAGE_SIZE) {
            throw new IllegalArgumentException(("Invalid page size"));
        }
        this.data = data;
        this.buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    }

    // - Init by method
    public void initEmpty(int pageId, PageType type) {
        initHeader(pageId, type);
    }


    // Header Initialization
    private void initHeader(int pageId, PageType type) {
        setPageId(pageId);
        setPageType(type);
        setSlotCount((short) 0);

        // Records start right after the header
        setFreeSpaceOffset((short) HEADER_SIZE);

        // Entire page except header is free
        setFreeSpaceSize((short) (PAGE_SIZE - HEADER_SIZE));

        setChecksum(0);
    }

    // Header getters setters
    public int getPageId() {
        // Page Id data is store at ${PAGE_ID_OFFSET} offset
        return buffer.getInt(PAGE_ID_OFFSET);
    }

    private void setPageId(int pageId) {
        buffer.putInt(PAGE_ID_OFFSET, pageId);
    }

    public PageType getPageType() {
        return PageType.from(buffer.get(PAGE_TYPE_OFFSET));
    }

    private void setPageType(PageType pageType) {
        buffer.put(PAGE_TYPE_OFFSET, pageType.code);
    }

    public short getSlotCount() {
        return buffer.getShort(SLOT_COUNT_OFFSET);
    }

    private void setSlotCount(short slotCount) {
        buffer.putShort(SLOT_COUNT_OFFSET, slotCount);
    }

    public short getFreeSpaceOffset() {
        return buffer.getShort(FREE_SPACE_OFFSET_OFFSET);
    }

    private void setFreeSpaceOffset(short offset) {
        buffer.putShort(FREE_SPACE_OFFSET_OFFSET, offset);
    }

    public short getFreeSpace() {
        return buffer.getShort(FREE_SPACE_OFFSET_OFFSET);
    }

    private void setFreeSpace(short freeSpace) {
        buffer.putShort(FREE_SPACE_OFFSET_OFFSET, freeSpace);
    }

    public short getFreeSpaceSize() {
        return buffer.getShort(FREE_SPACE_SIZE_OFFSET);
    }

    private void setFreeSpaceSize(short freeSpaceSize) {
        buffer.putShort(FREE_SPACE_SIZE_OFFSET, freeSpaceSize);
    }

    private void setChecksum(int checksum) {
        buffer.putInt(CHECKSUM_OFFSET, checksum);
    }

    /**
     * Slot management
     */

    // Slot start at the bottom
    private int slotPosition(int slotIndex) {
        return PAGE_SIZE - ((slotIndex + 1) * SLOT_SIZE);
    }

    public Slot getSlot(int slotIndex) {
        int pos = slotPosition(slotIndex);
        short offset = buffer.getShort(pos);
        short length = buffer.getShort(pos + 2);
        return new Slot(offset, length);
    }

    private void setSlot(int slotIndex, short offset, short length) {
        int pos = slotPosition(slotIndex);
        buffer.putShort(pos, offset);
        buffer.putShort(pos + 2, length);
    }

    // Get "deleted" slot
    private int findDeletedSlot() {
        int slotCount = getSlotCount();
        for (int i = 0; i < slotCount; i++) {
            Slot slot = getSlot(i);
            if (slot.getOffset() < 0) {
                return i;
            }
        }
        return -1;
    }

    public boolean hasSpaceFor(int recordSize) {
        return getFreeSpaceLeft() >= recordSize + SLOT_SIZE;
    }

    /**
     * Read a record from data
     */
    public byte[] readRecord(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= getSlotCount()) {
            throw new IllegalArgumentException(("Invalid slot index"));
        }

        Slot slot = getSlot(slotIndex);
        short offset = slot.getOffset();
        short length = slot.getLength();

        // Deleted slot
        if (offset < 0) {
            return null;
        }

        // init record data with known length
        byte[] record = new byte[length];
        // Pipe bytes to record variable
        buffer.position(offset).get(record);

        return record;
    }

    /**
     * Insert a raw record into the page.
     */
    public int insertRecord(byte[] recordBytes) {
        int recordSize = recordBytes.length;

        if (!hasSpaceFor(recordSize)) {
            throw new IllegalStateException("Not enough space to insert record");
        }

        // get slot index
        int slotIndex = findDeletedSlot();
        // if no "deleted" slot found, create new slot
        if (slotIndex == -1) {
            slotIndex = getSlotCount();
            setSlotCount((short) (slotIndex + 1));
        }
        // record grows from top
        short recordOffset = getFreeSpaceOffset();

        // Write record bytes
        buffer.position(recordOffset).put(recordBytes);

        // Write slot data (grows from bottom)
        setSlot(slotIndex, recordOffset, (short) recordSize);
        // Update free space index for the next record bytes to start
        setFreeSpaceOffset((short) (recordOffset + recordSize));

        return slotIndex;
    }

    /**
     * Marks slot as invalid.
     */
    public void deleteRecord(int slotIndex) {
        int pos = slotPosition(slotIndex);
        buffer.putShort(pos, (short) -1);
        buffer.putShort(pos + 2, (short) 0);
    }

    public int getFreeSpaceLeft() {
        int slotDirStart = slotPosition(getSlotCount());
        int freeSpaceOffset = getFreeSpaceOffset();
        return slotDirStart - freeSpaceOffset;
    }

    public byte[] getData() {
        return data;
    }
}