package sql.storage;

import sql.page.Page;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DiskManager {
    private final RandomAccessFile file;
    private final int pageSize;

    public DiskManager(String filePath, int pageSize) throws IOException {
        this.file = new RandomAccessFile(filePath, "rw");
        this.pageSize = pageSize;
    }

    public void writePage(int pageId, byte[] data) throws IOException {
        if (data.length > pageSize) {
            throw new IllegalArgumentException("Invalid page size");
        }

        long offset = (long) pageId * pageSize;
        file.seek(offset);
        file.write(data);
    }

    public Page getPage(int pageId) throws IOException {
        byte[] pageBytes = new byte[Page.PAGE_SIZE];
        readPage(pageId, pageBytes);
        return new Page(pageBytes);
    }

    public void readPage(int pageId, byte[] data) throws IOException {
        if (data.length != pageSize) {
            throw new IllegalArgumentException("Invalid page size");
        }

        long offset = (long) pageId * pageSize;
        file.seek(offset);
        // pipe file content starts from the above offset into data variable
        file.readFully(data);
    }

    public void close() throws IOException {
        file.close();
    }
}
