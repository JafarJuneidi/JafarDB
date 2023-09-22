package org.jafar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class Collection {
    private byte[] name;
    private long root;
    private long counter;
    private Transaction transaction;

    public Collection() {}

    public Collection(byte[] name, long root) {
        this.name = name;
        this.root = root;
    }

    public byte[] getName() { return this.name; }
    public long getRoot() { return this.root; }
    public long getCounter() { return this.counter; }
    public Transaction getTransaction() { return this.transaction; }

    public void setRoot(long root) { this.root = root; }
    public void setTransaction(Transaction transaction) { this.transaction = transaction; }
    public void setName(byte[] name) { this.name = name; }
    public void setCounter(long counter) { this.counter = counter; }

    public Optional<Item> find(byte[] key) throws IOException {
        Node node = transaction.getNode(root);
        var result = node.findKey(key, true);

        if (result.getIndex() == -1) {
            return Optional.empty();
        }
        return Optional.of(result.getNode().getItems().get(result.getIndex()));
    }

    public List<Item> findAll() throws IOException {
        Node rootNode = transaction.getNode(root);
        List<Item> items = new LinkedList<>();
        rootNode.findAll(items);
        return items;
    }

    public void put(byte[] key, byte[] value) throws IOException, Constants.WriteInsideReadTransactionException {
        if (!transaction.getWrite()) {
            throw new Constants.WriteInsideReadTransactionException();
        }
        Item item = new Item(key, value);

        // On first insertion, the root node does not exist, so it should be created
        Node root;
        if (this.root == 0) {
            // todo
            List<Item> newItems = new ArrayList<>();
            newItems.add(item);
            root = transaction.writeNode(transaction.newNode(newItems, new ArrayList<>()));
            this.root = root.getPageNum();
            return;
        } else {
            root = transaction.getNode(this.root);
        }

        // Find the path to the node where the insertion should happen
        Node.FindResult findResult = root.findKey(item.key(), false);
        int insertionIndex = findResult.getIndex();
        Node nodeToInsertIn = findResult.getNode();
        List<Integer> ancestorsIndexes = findResult.getAncestorIndexes();

        // If key already exists
        if (nodeToInsertIn.getItems() != null && insertionIndex < nodeToInsertIn.getItems().size() &&
                Arrays.equals(nodeToInsertIn.getItems().get(insertionIndex).key(), key)) {
            // Hmm Test this
            nodeToInsertIn.getItems().set(insertionIndex, item);
        } else {
            // Add item to the leaf node
            nodeToInsertIn.addItem(item, insertionIndex);
        }
        nodeToInsertIn.writeNode(nodeToInsertIn);

        List<Node> ancestors = getNodes(ancestorsIndexes);

        // Rebalance the nodes all the way up. Start From one node before the last and go all the way up. Exclude root.
        for (int i = ancestors.size() - 2; i >= 0; i--) {
            Node parentNode = ancestors.get(i);
            Node node = ancestors.get(i + 1);
            int nodeIndex = ancestorsIndexes.get(i + 1);
            if (node.isOverPopulated()) {
                parentNode.split(node, nodeIndex);
            }
        }

        // Handle root
        Node rootNode = ancestors.get(0);
        if (rootNode.isOverPopulated()) {
            // todo
            List<Long> list = new ArrayList<>();
            list.add(rootNode.getPageNum());
            Node newRoot = transaction.newNode(new ArrayList<>(), list);
            newRoot.split(rootNode, 0);

            // Commit newly created root
            newRoot = transaction.writeNode(newRoot);
            this.root = newRoot.getPageNum();
        }
    }

    public List<Node> getNodes(List<Integer> indexes) throws IOException {
        Node root = transaction.getNode(this.root);

        List<Node> nodes = new ArrayList<>();
        nodes.add(root);

        Node child = root;
        for (int i = 1; i < indexes.size(); i++) {
            child = transaction.getNode(child.getChildNodes().get(indexes.get(i)));
            if (child == null) {
                return null;  // Or you could throw an exception
            }
            nodes.add(child);
        }
        return nodes;
    }

    public boolean remove(byte[] key) throws IOException, Constants.WriteInsideReadTransactionException {
        if (!transaction.getWrite()) {
            throw new Constants.WriteInsideReadTransactionException();
        }

        // Find the path to the node where the deletion should happen
        Node rootNode = transaction.getNode(root);
        Node.FindResult result = rootNode.findKey(key, true);
        int removeItemIndex = result.getIndex();
        Node nodeToRemoveFrom = result.getNode();
        List<Integer> ancestorIndexes = result.getAncestorIndexes();

        if (removeItemIndex == -1) {
            return false;
        }

        if (nodeToRemoveFrom.isLeaf()) {
            nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex);
        } else {
            List<Integer> affectedNodes = nodeToRemoveFrom.removeItemFromInternal(removeItemIndex);
            ancestorIndexes.addAll(affectedNodes);
        }

        List<Node> ancestors = getNodes(ancestorIndexes);

        // Rebalance the nodes all the way up, excluding the root.
        for (int i = ancestors.size() - 2; i >= 0; i--) {
            Node parentNode = ancestors.get(i);
            Node node = ancestors.get(i+1);
            if (node.isUnderPopulated()) {
                    parentNode.rebalanceRemove(node, ancestorIndexes.get(i+1));
            }
        }

        rootNode = ancestors.get(0);
        // If the root has no items after rebalancing and has child nodes, adjust root.
        if (rootNode.getItems().isEmpty() && !rootNode.getChildNodes().isEmpty()) {
            this.root = ancestors.get(1).getPageNum();
        }

        return true;
    }

    public void deserialize(Item item) {
        this.name = item.key();

        byte[] value = item.value();
        if (value.length != 0) {
            ByteBuffer buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
            this.root = buffer.getLong();
            this.counter = buffer.getLong();
        }
    }

    public Item serialize() {
        // todo
        ByteBuffer buffer = ByteBuffer.allocate(Constants.CollectionSize).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(root);
        buffer.putLong(counter);

        return new Item(name, buffer.array());
    }
}
