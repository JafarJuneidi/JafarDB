package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Transaction {
    private HashMap<Long, Node> dirtyNodes;
    private ArrayList<Long> pagesToDelete;
    // new pages allocated during the transaction. They will be released if rollback is called.
    private ArrayList<Long> allocatedPageNums;
    private boolean write;
    private DB db;

    public DB getDb() {
        return db;
    }

    public Transaction(DB db, boolean write) {
        this.dirtyNodes = new HashMap<>();
        this.pagesToDelete = new ArrayList<>();
        this.allocatedPageNums = new ArrayList<>();
        this.write = write;
        this.db = db;
    }

    public boolean getWrite() {
        return write;
    }

    public void rollback() {
        if (!write) {
            db.getRwlock().readLock().unlock();
            return;
        }

        dirtyNodes = null;
        pagesToDelete = null;
        for (long pageNum: allocatedPageNums) {
            db.getDal().getFreelist().releasePage(pageNum);
        }

        allocatedPageNums = null;
        db.getRwlock().writeLock().unlock();
    }

    public void commit() throws IOException {
        if (!write) {
            db.getRwlock().readLock().unlock();
            return;
        }

        for (Node node: dirtyNodes.values()) {
            db.getDal().writeNode(node);
        }

        for (long pageNum: pagesToDelete) {
            db.getDal().deleteNode(pageNum);
        }

        db.getDal().writeFreelist();
        dirtyNodes = null;
        pagesToDelete = null;
        allocatedPageNums = null;
        db.getRwlock().writeLock().unlock();
    }

    public Node newNode(List<Item> items, List<Long> childNodes) {
        Node node = new Node();
        node.setItems(items);
        node.setChildNodes(childNodes);
        node.setPageNum(db.getDal().getFreelist().getNextPage());
        node.setTransaction(this);

        node.getTransaction().allocatedPageNums.add(node.getPageNum());
        return node;
    }

    public Node writeNode(Node node) {
        dirtyNodes.put(node.getPageNum(), node);
        node.setTransaction(this);
        return node;
    }

    public Node getNode(long pageNum) throws IOException {
        if (dirtyNodes.containsKey(pageNum)) {
            return dirtyNodes.get(pageNum);
        }

        Node node = db.getDal().getNode(pageNum);
        node.setTransaction(this);
        return node;
    }

    public void deleteNode(Node node) {
        pagesToDelete.add(node.getPageNum());
    }
}
