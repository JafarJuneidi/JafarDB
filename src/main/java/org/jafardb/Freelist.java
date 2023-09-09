package org.jafardb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Freelist {
    private long maxPage; // Holds the maximum page allocated. maxPage*PageSize = fileSize
    private List<Long> releasedPages; // Pages that were previously allocated but are now free
    private static final long metaPage = 0; // max metaPage used by DB, currently only page 0

    public Freelist() {
        this.maxPage = metaPage;
        this.releasedPages = new ArrayList<>();
    }

    public long getMaxPage() { return maxPage; }
    public List<Long> getReleasedPages() { return releasedPages; }

    public void setMaxPage(long maxPage) { this.maxPage = maxPage; }
    public void setReleasedPages(List<Long> releasedPages) { this.releasedPages = releasedPages; }


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

    public void serialize(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);

        byteBuffer.putShort((short) maxPage);
        byteBuffer.putShort((short) releasedPages.size());

        for (Long page : releasedPages) {
            byteBuffer.putLong(page);
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Freelist freelist = (Freelist) o;
        return maxPage == freelist.maxPage && Objects.equals(releasedPages, freelist.releasedPages);
    }
}

