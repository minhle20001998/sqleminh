package sql.page;

public enum PageType {
    DATA((byte) 1),
    INDEX((byte) 2),
    META((byte) 3);

    public final byte code;

    PageType(byte code) {
        this.code = code;
    }

    public static PageType from(byte code) {
        for (PageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown page type: " + code);
    }
}