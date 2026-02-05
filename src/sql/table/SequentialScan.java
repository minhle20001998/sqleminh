package sql.table;

import sql.buffer.BufferPool;
import sql.page.Page;
import sql.page.Slot;

import java.io.IOException;

public class SequentialScan {

    private final BufferPool bufferPool;
    private final int firstPageId;
    private final int lastPageId;

    private int currentPageId;
    private int currentSlotId;

    private Page currentPage;
    private boolean finished = false;

    public SequentialScan(BufferPool bufferPool, int firstPageId, int lastPageId) throws IOException {
        this.bufferPool = bufferPool;
        this.firstPageId = firstPageId;
        this.lastPageId = lastPageId;

        this.currentPageId = firstPageId;
        this.currentSlotId = 0;

        this.currentPage = bufferPool.fetchPage(currentPageId);
    }

    /**
     * Returns the next record, or null if scan is finished.
     */
    public byte[] next() throws IOException {
        if (finished) {
            return null;
        }

        while (true) {

            while (currentSlotId < currentPage.getSlotCount()) {
                Slot slot = currentPage.getSlot(currentSlotId);
                int slotId = currentSlotId;
                currentSlotId++;

                if (slot.isDeleted()) {
                    continue;
                }

                return currentPage.readRecord(slotId);
            }

            bufferPool.unpinPage(currentPageId, false);

            currentPageId++;
            currentSlotId = 0;

            if (currentPageId > lastPageId) {
                finished = true;
                currentPage = null;
                return null;
            }

            currentPage = bufferPool.fetchPage(currentPageId);
        }
    }

    /**
     * Must be called by user when scan is done.
     */
    public void close() throws IOException {
        if (!finished && currentPage != null) {
            bufferPool.unpinPage(currentPageId, false);
        }
        finished = true;
        currentPage = null;
    }
}
