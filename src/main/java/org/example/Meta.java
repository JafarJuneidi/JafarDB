package org.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Meta {
    public long freelistPage;
    public long root;
    public static final long MetaPageNum = 0;

    public Meta(long freelistPage) {
        this.freelistPage = freelistPage;
    }

    public Meta() {
        this.freelistPage = MetaPageNum;
    }

    public void serialize(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(root);
        byteBuffer.putLong(freelistPage);
    }

    public void deserialize(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.root = byteBuffer.getLong();
        this.freelistPage = byteBuffer.getLong();
    }
}
