package sql.page;

public class Slot {
    public static final int Size = 4;

    // Where the record starts in the page
    private final short offset;
    // How many bytes the record uses
    private final short length;

    public Slot(short offset, short length) {
        this.offset = offset;
        this.length = length;
    }

    public short getOffset() {
        return offset;
    }

    public short getLength() {
        return length;
    }

    public boolean isDeleted() {
        return offset < 0;
    }

    @Override
    public String toString() {
        return "Slot[offset=" + offset + ", length=" + length + "]";
    }
}