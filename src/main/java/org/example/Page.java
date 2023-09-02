package org.example;

public class Page {
    private long num;
    private byte[] data;

    public Page(long num, byte[] data) {
        this.num = num;
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }
}
