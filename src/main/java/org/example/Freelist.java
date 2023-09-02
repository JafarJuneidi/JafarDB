package org.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class Freelist {
    private long maxPage; // Holds the maximum page allocated. maxPage*PageSize = fileSize
    private final List<Long> releasedPages; // Pages that were previously allocated but are now free
    private static final long metaPage = 0; // max metaPage used by DB, currently only page 0

    public Freelist() {
        this.maxPage = metaPage;
        this.releasedPages = new ArrayList<>();
    }

    public long getNextPage() {
        if (!releasedPages.isEmpty()) {
            long pageID = releasedPages.get(releasedPages.size() - 1);
            releasedPages.remove(releasedPages.size() - 1);
            return pageID;
        }
        maxPage += 1;
        return maxPage;
    }

    public void releasePage(long page) {
        releasedPages.add(page);
    }

    public byte[] serialize() {
        int pageNumSize = Long.BYTES;  // Size of a long in bytes (8 bytes)
        ByteBuffer buf = ByteBuffer.allocate(4 + pageNumSize * releasedPages.size())
                .order(ByteOrder.LITTLE_ENDIAN); // Little-endian order

        buf.putShort((short) maxPage);  // Assuming maxPage can fit into a short
        buf.putShort((short) releasedPages.size());  // Number of released pages

        for (long page : releasedPages) {
            buf.putLong(page);
        }

        return buf.array();
    }

    public void deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        maxPage = buf.getShort();

        int releasedPagesCount = buf.getShort();
        releasedPages.clear();
        for (int i = 0; i < releasedPagesCount; i++) {
            releasedPages.add(buf.getLong());
        }
    }
}

