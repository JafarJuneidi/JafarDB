package org.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Meta {
    // The database has a root collection that holds all the collections in the database. It is called root and the
    // root property of meta holds page number containing the root of collections collection. The keys are the
    // collections names and the values are the page number of the root of each collection. Then, once the collection
    // and the root page are located, a search inside a collection can be made.
    public long freelistPage;

    public void setRoot(long root) {
        this.root = root;
    }

    public long root;
    public static final long MetaPageNum = 0;

    public Meta(long freelistPage) {
        this.freelistPage = freelistPage;
    }

    public Meta() {
        this.freelistPage = MetaPageNum;
    }

    public byte[] serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(root);
        byteBuffer.putLong(freelistPage);

        return byteBuffer.array();
    }

    public void deserialize(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.root = byteBuffer.getLong();
        this.freelistPage = byteBuffer.getLong();
    }
}
