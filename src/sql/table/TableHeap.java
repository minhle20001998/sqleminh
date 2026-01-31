package sql.table;

import sql.buffer.BufferPool;
import sql.page.Page;
import sql.page.PageType;
import sql.record.RecordId;

import java.io.IOException;

public class TableHeap {

    private final BufferPool bufferPool;
    private int firstPageId;
    private int lastPageId;

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
                bufferPool.unpinPage(firstPageId, false);
                return new RecordId(pageId, slotId);
            }

            bufferPool.unpinPage(firstPageId, false);

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
        bufferPool.unpinPage(firstPageId, false);

        return data;
    }

    public void delete(RecordId rid) throws IOException {
        int pageId = rid.getPageId();

        Page page = bufferPool.fetchPage(pageId);
        page.deleteRecord(rid.getSlotId());
        bufferPool.unpinPage(firstPageId, true);
    }
}
