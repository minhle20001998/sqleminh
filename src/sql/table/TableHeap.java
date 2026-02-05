package sql.table;

import sql.buffer.BufferPool;
import sql.page.Page;
import sql.page.PageType;
import sql.page.Slot;
import sql.record.RecordId;

import java.io.IOException;

public class TableHeap {

    private final BufferPool bufferPool;

    private int firstPageId;
    private int lastPageId;

    public int getFirstPageId() {
        return firstPageId;
    }

    public int getLastPageId() {
        return lastPageId;
    }

    public TableHeap(BufferPool bufferPool, int firstPageId) throws IOException {
        this.bufferPool = bufferPool;
        this.firstPageId = firstPageId;
        this.lastPageId = firstPageId;

        Page page = bufferPool.fetchPage(firstPageId);
        bufferPool.unpinPage(firstPageId, false);
    }

    public RecordId insert(byte[] recordBytes) throws IOException {

        int pageId = firstPageId;

        while (true) {
            Page page = bufferPool.fetchPage(pageId);

            if (page.hasSpaceFor(recordBytes.length)) {
                short slotId = (short) page.insertRecord(recordBytes);
                bufferPool.unpinPage(pageId, true);
                return new RecordId(pageId, slotId);
            }

            bufferPool.unpinPage(pageId, false);

            // Move to next page or create one
            if (pageId == lastPageId) {
                int newPageId = lastPageId + 1;
                Page newPage = bufferPool.fetchPage(newPageId);
                newPage.initEmpty(newPageId, PageType.DATA);

                bufferPool.unpinPage(newPageId, true);

                lastPageId = newPageId;
            }

            pageId++;
        }
    }

    public byte[] read(RecordId rid) throws IOException {
        int pageId = rid.getPageId();

        Page page = bufferPool.fetchPage(pageId);
        byte[] data = page.readRecord(rid.getSlotId());
        bufferPool.unpinPage(pageId, false);

        return data;
    }

    public RecordId update(RecordId rid, byte[] newData) throws IOException {
        int pageId = rid.getPageId();
        short slotId = rid.getSlotId();

        Page page = bufferPool.fetchPage(pageId);
        Slot slot = page.getSlot(slotId);

        // overwrite in-place if it fits
        if (!slot.isDeleted() && newData.length <= slot.getLength()) {
            page.writeRecord(slotId, newData);
            bufferPool.unpinPage(pageId, true);
            return rid;
        }
        // delete old data and replace
        else {
            System.out.println("Delete old data and replace");
            page.deleteRecord(slotId);
            bufferPool.unpinPage(pageId, true);

            return insert(newData);
        }
    }

    public void delete(RecordId rid) throws IOException {
        int pageId = rid.getPageId();

        Page page = bufferPool.fetchPage(pageId);
        page.deleteRecord(rid.getSlotId());
        bufferPool.unpinPage(pageId, true);
    }
}
