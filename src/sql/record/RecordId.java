package sql.record;

import java.util.Objects;

public class RecordId {
    private final int pageId;
    private final short slotId;

    public RecordId(int pageId, short slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }

    public int getPageId() {
        return pageId;
    }

    public short getSlotId() {
        return slotId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecordId thatRecordId)) return false;
        return pageId == thatRecordId.pageId && slotId == thatRecordId.slotId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId, slotId);
    }

    @Override
    public String toString() {
        return "RecordId{" + "pageId=" + pageId + ", slotId=" + slotId + '}';
    }
}
