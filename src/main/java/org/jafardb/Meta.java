package org.jafardb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class Meta {
    // The database has a root collection that holds all the collections in the database. It is called root and the
    // root property of meta holds page number containing the root of collections collection. The keys are the
    // collections names and the values are the page number of the root of each collection. Then, once the collection
    // and the root page are located, a search inside a collection can be made.
    private long freelistPage;
    private long root;
    public static final long MetaPageNum = 0;
    public static final int MagicNumber = 0xD00DB00D;


    public Meta() {
        this.freelistPage = MetaPageNum;
    }
    public Meta(long freelistPage) {
        this.freelistPage = freelistPage;
    }

    public void setRoot(long root) { this.root = root; }
    public void setFreelistPage(long freelistPage) { this.freelistPage = freelistPage; }

    public long getRoot() { return root; }
    public long getFreelistPage() { return freelistPage; }

    public void serialize(byte[] buff) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buff).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(MagicNumber);
        byteBuffer.putLong(root);
        byteBuffer.putLong(freelistPage);
    }

    public void deserialize(byte[] data) throws Constants.NotJafarDBFile {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int magicNumberRes = byteBuffer.getInt();
        if (magicNumberRes != MagicNumber) {
            throw new Constants.NotJafarDBFile();
        }
        this.root = byteBuffer.getLong();
        this.freelistPage = byteBuffer.getLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Meta meta = (Meta) o;
        return freelistPage == meta.freelistPage && root == meta.root;
    }
}
