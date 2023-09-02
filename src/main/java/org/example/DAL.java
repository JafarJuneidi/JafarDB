package org.example;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DAL {
    private RandomAccessFile file;
    private int pageSize;
    private Meta meta;
    private Freelist freelist;
    private static final int pageNumSize = 8;

    public DAL(String path) throws IOException {
        this.pageSize = 4096;

        File file = new File(path);
        if (file.exists()) {
            this.file = new RandomAccessFile(path, "rw");

            try {
                this.meta = readMeta();
                this.freelist = readFreelist();
            } catch (IOException e) {
                close();  // Assuming you have a close method for DAL
                throw e;
            }

        } else {
            this.file = new RandomAccessFile(path, "rw");
            this.freelist = new Freelist();
            this.meta = new Meta(freelist.getNextPage());
            writeFreelist();
            writeMeta();
        }
    }

    public void close() throws IOException {
        writeFreelist();
        writeMeta();
        if (file != null) {
            file.close();
        }
    }

    public Page allocateEmptyPage() {
        return new Page(freelist.getNextPage(), new byte[pageSize]);
    }

    public Page allocateEmptyPage(long pageNum) {
        return new Page(pageNum, new byte[pageSize]);
    }

    public Page readPage(long pageNum) throws IOException {
        Page page = allocateEmptyPage(pageNum);
        long offset = pageNum * pageSize;
        file.seek(offset);
        file.read(page.getData());
        return page;
    }

    public void writePage(Page page) throws IOException {
        long offset = page.getNum() * pageSize;
        try {
            file.seek(offset);
            file.write(page.getData());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void writeMeta() throws IOException {
        Page p = allocateEmptyPage(Meta.MetaPageNum);
        meta.serialize(p.getData());

        writePage(p);
    }

    public Meta readMeta() throws IOException {
        Page p = readPage(Meta.MetaPageNum);

        Meta meta = new Meta();
        meta.deserialize(p.getData());
        return meta;
    }

    public Freelist readFreelist() throws IOException {
        Page p = readPage(this.meta.freelistPage);

        Freelist freelist = new Freelist();
        freelist.deserialize(p.getData());
        return freelist;
    }

    public Page writeFreelist() throws IOException {
        Page p = allocateEmptyPage(meta.freelistPage);
        p.setData(freelist.serialize());

        writePage(p);
        return p;
    }

    public void releasePage(Page p) {
        freelist.releasePage(p.getNum());
    }
}