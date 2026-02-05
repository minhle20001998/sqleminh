package sql.buffer;

import sql.page.Page;
import sql.page.PageType;
import sql.storage.DiskManager;

import java.io.IOException;
import java.util.*;

public class BufferPool {
    private final int maxFrames;
    private final DiskManager diskManager;

    // Mapping pageId - frame
    private final Map<Integer, Frame> pageTable;

    // FIFO queue of pageIds;
    private final Queue<Integer> fifoQueue;

    public BufferPool(int maxFrames, DiskManager diskManager) {
        this.maxFrames = maxFrames;
        this.diskManager = diskManager;
        this.pageTable = new HashMap<>();
        this.fifoQueue = new LinkedList<>();
    }

    // get Page from this Buffer Pool
    public Page fetchPage(int pageId) throws IOException {
        if (pageTable.containsKey(pageId)) {
            Frame frame = pageTable.get(pageId);
            frame.pin();
            return frame.getPage();
        }

        if (pageTable.size() >= maxFrames) {
            evictPage();
        }

        Page page;

        if (diskManager.pageExists(pageId)) {
            page = diskManager.getPage(pageId);
        } else {
            page = new Page(pageId, PageType.DATA);
            diskManager.writePage(pageId, page.getData());
        }

        Frame frame = new Frame(page);
        frame.pin();

        pageTable.put(pageId, frame);
        fifoQueue.add(pageId);

        return page;
    }

    /**
     * Unpin a page.
     * If dirty, mark it so it will be flushed later.
     */
    public void unpinPage(int pageId, boolean isDirty) throws IOException {
        Frame frame = pageTable.get(pageId);
        if (frame == null) {
            throw new IllegalArgumentException("Page not found in buffer pool: " + pageId);
        }

        if (isDirty) {
            frame.markDirty();
        }

        frame.unpin();
    }

    // Write a page back to disk if dirty
    public void flushPage(int pageId) throws IOException {
        Frame frame = pageTable.get(pageId);
        if (frame == null) {
            return;
        }

        if (frame.isDirty()) {
            Page page = frame.getPage();
            diskManager.writePage(pageId, page.getData());
            frame.clearDirty();
        }
    }

    // Flush all dirty pages.
    public void flushAll() throws IOException {
        for (int pageId : pageTable.keySet()) {
            flushPage(pageId);
        }
    }

    // Evict page out of queue
    private void evictPage() throws IOException {

        int attempts = fifoQueue.size();

        while (attempts-- > 0) {
            int pageId = fifoQueue.poll();
            Frame victim = pageTable.get(pageId);
            if (victim.isPinned()) {
                fifoQueue.add(pageId);
                continue;
            }

            if (victim.isDirty()) {
                Page page = victim.getPage();
                diskManager.writePage(pageId, page.getData());
            }

            pageTable.remove(pageId);
            return;
        }

        // If we reach here, all pages are pinned
        throw new IllegalStateException("All pages are pinned - No unpinned pages available for eviction");
    }

    public Map<Integer, Frame> getPageTable() {
        return pageTable;
    }
}
