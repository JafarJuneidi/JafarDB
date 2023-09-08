package org.example;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class DAL {
    private final RandomAccessFile file;
    private final Options options;
    private final Meta meta;
    private final Freelist freelist;
    private static final int pageNumSize = 8;

    public Freelist getFreelist() {
        return freelist;
    }

    public DAL(String path, Options options) throws IOException {
        this.options = options;

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

//            collectionsNode, err := dal.writeNode(NewNodeForSerialization([]*Item{}, []pgnum{}))
            Node collectionsNode = writeNode(new Node(new ArrayList<>(), new ArrayList<>()));
            this.meta.setRoot(collectionsNode.getPageNum());

            writeFreelist();
            writeMeta();
        }
    }

    public void close() throws IOException {
        writeFreelist();
//        writeMeta();
        if (file != null) {
            file.close();
        }
    }

    public Page allocateEmptyPage() {
        return new Page(new byte[options.getPageSize()]);
    }

    public Page readPage(long pageNum) throws IOException {
        Page page = allocateEmptyPage();
        page.setNum(pageNum);
        long offset = pageNum * options.getPageSize();
        file.seek(offset);
        file.read(page.getData());
        return page;
    }

    public void writePage(Page page) throws IOException {
        long offset = page.getNum() * options.getPageSize();
        try {
            file.seek(offset);
            file.write(page.getData());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void writeMeta() throws IOException {
        Page page = allocateEmptyPage();
        page.setNum(Meta.MetaPageNum);
        page.setData(meta.serialize());

        writePage(page);
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

    public void writeFreelist() throws IOException {
        Page page = allocateEmptyPage();
        page.setNum(meta.freelistPage);
        page.setData(freelist.serialize());

        writePage(page);
    }

    public void releasePage(long pageNum) {
        freelist.releasePage(pageNum);
    }

    public Node getNode(long pageNum) throws IOException {
        Page p = readPage(pageNum);
        Node node = new Node();
        node.setPageNum(pageNum);
        node.deserialize(p.getData());
        node.setDal(this);
        return node;
    }

    public Node writeNode(Node node) throws IOException {
        Page page = allocateEmptyPage();
        if (node.getPageNum() == 0) {
            page.setNum(freelist.getNextPage());
            node.setPageNum(page.getNum());
        } else {
            page.setNum(node.getPageNum());
        }

        page.setData(node.serialize(page.getData()));

        writePage(page);
        return node;
    }

    public void deleteNode(long pageNum) {
        releasePage(pageNum);
    }

    public long getRoot() {
        return meta.root;
    }

    public float maxThreshold() {
        return options.getMaxFillPercent() * options.getPageSize();
    }

    public boolean isOverPopulated(Node node) {
        return node.nodeSize() > maxThreshold();
    }

    public float minThreshold() {
        return options.getMinFillPercent() * (float) options.getPageSize();
    }

    public boolean isUnderPopulated(Node node) {
        return (float) node.nodeSize() < minThreshold();
    }

    public int getSplitIndex(Node node) {
        int size = Constants.nodeHeaderSize;

        for (int i = 0; i < node.getItems().size(); i++) {
            size += node.elementSize(i);

            // if we have a big enough page size (more than minimum), and didn't reach the last node, which means we can
            // spare an element
            if ( size > minThreshold() && i < node.getItems().size() - 1) {
                return i + 1;
            }
        }

        return -1;
    }

    public Node newNode(List<Item> items, List<Long> childNodes) {
        Node node = new Node();
        node.setItems(items);
        node.setChildNodes(childNodes);
        node.setPageNum(freelist.getNextPage());
        node.setDal(this);
        return node;
    }
}