package sql.buffer;

import sql.page.Page;

public class Frame {

    private Page page;
    private int pinCount;
    private boolean dirty;

    public Frame(Page page) {
        // page in memory
        this.page = page;
        // how many users using it
        this.pinCount = 0;
        // should this page be flushed to disk
        this.dirty = false;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public int getPinCount() {
        return pinCount;
    }

    public void pin() {
        pinCount++;
    }

    public void unpin() {
        if (pinCount <= 0) {
            throw new IllegalStateException("Unpin called on frame with illegal pin count");
        }
        pinCount--;
    }

    public boolean isPinned() {
        return pinCount > 0;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void markDirty() {
        dirty = true;
    }

    public void clearDirty() {
        dirty = false;
    }
}
