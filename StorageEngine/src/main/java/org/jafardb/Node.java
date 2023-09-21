package org.jafardb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class Node {
    private DAL dal;
    private long pageNum;
    private List<Item> items;
    private List<Long> childNodes;
    private Transaction transaction;

    public Node() {
        this.items = new ArrayList<>();
        this.childNodes = new ArrayList<>();
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public Node(List<Item> items, List<Long> childNodes) {
        this.items = items;
        this.childNodes = childNodes;
    }

    public void setPageNum(long pageNum) {
        this.pageNum = pageNum;
    }

    public long getPageNum() {
        return pageNum;
    }

    public boolean isLeaf() {
        return childNodes.isEmpty();
    }

    public void serialize(byte[] buf) {
        int leftPos = 0;
        int rightPos = buf.length - 1;

        boolean isLeaf = isLeaf();

        buf[leftPos] = (byte) (isLeaf ? 1 : 0);
        leftPos++;

        ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putShort(leftPos, (short) items.size());
        leftPos += 2;

        for (int i = 0; i < items.size(); i++) {
            Item item = items.get(i);
            if (!isLeaf) {
                long childNode = childNodes.get(i);
                ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(leftPos, childNode);
                leftPos += 8; // assuming pageNumSize is 8 (long)
            }

            int keyLength = item.key().length;
            int valueLength = item.value().length;

            int offset = rightPos - keyLength - valueLength - 2;
            ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putShort(leftPos, (short) offset);
            leftPos += 2;

            rightPos -= valueLength;
            System.arraycopy(item.value(), 0, buf, rightPos, valueLength);

            rightPos--;
            buf[rightPos] = (byte) valueLength;

            rightPos -= keyLength;
            System.arraycopy(item.key(), 0, buf, rightPos, keyLength);

            rightPos--;
            buf[rightPos] = (byte) keyLength;
        }

        if (!isLeaf) {
            long lastChildNode = childNodes.get(childNodes.size() - 1);
            ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(leftPos, lastChildNode);
        }
    }

    public void deserialize(byte[] buf) {
        int leftPos = 0;

        // Read header
        boolean isLeaf = (buf[0] & 0xFF) == 1;

        short itemsCount = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getShort(1);
        leftPos += 3;

        if (!isLeaf) {
            childNodes = new ArrayList<>();
        }

        items = new ArrayList<>();

        // Read body
        for (int i = 0; i < itemsCount; i++) {
            if (!isLeaf) {
                long pageNum = ByteBuffer.wrap(buf, leftPos, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
                leftPos += 8;

                childNodes.add(pageNum);
            }

            // Read offset
            int offset = ByteBuffer.wrap(buf, leftPos, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
            leftPos += 2;

            int keyLength = buf[offset] & 0xFF;
            offset++;

            byte[] key = new byte[keyLength];
            System.arraycopy(buf, offset, key, 0, keyLength);
            offset += keyLength;

            int valueLength = buf[offset] & 0xFF;
            offset++;

            byte[] value = new byte[valueLength];
            System.arraycopy(buf, offset, value, 0, valueLength);
            offset += valueLength;

            items.add(new Item(key, value));
        }

        if (!isLeaf) {
            long lastChildNode = ByteBuffer.wrap(buf, leftPos, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
            childNodes.add(lastChildNode);
        }
    }

    public Node writeNode(Node node) throws IOException {
        return this.transaction.writeNode(node);
    }

    public void writeNodes(Node... nodes) throws IOException {
        for (Node node : nodes) {
            this.writeNode(node);
        }
    }

    public Node getNode(long pageNum) throws IOException {
        return this.transaction.getNode(pageNum);
    }

    /**
     * Finds the key in the node.
     *
     * @param key the key to search for
     * @return a boolean indicating whether the key is present, and an index (either of the key, or where it should be)
     */
    public Pair<Boolean, Integer> findKeyInNode(byte[] key) {
        for (int i = 0; i < items.size(); i++) {
            Item existingItem = items.get(i);
            int res = compare(existingItem.key(), key);

            if (res == 0) {
                return new Pair<>(true, i);
            }

            // The key is bigger than the previous key, so it doesn't exist in the node, but may exist in child nodes.
            if (res > 0) {
                return new Pair<>(false, i);
            }
        }

        // The key isn't bigger than any of the keys which means it's in the last index.
        return new Pair<>(false, items.size());
    }

    /**
     * Compares two byte arrays.
     *
     * @param a the first byte array
     * @param b the second byte array
     * @return an int representing the comparison result
     */
    private int compare(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    public void setDal(DAL dal) {
        this.dal = dal;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    public void setChildNodes(List<Long> childNodes) {
        this.childNodes = childNodes;
    }

    public List<Long> getChildNodes() {
        return childNodes;
    }

    public record Pair<K, V>(K key, V value) {}

    public FindResult findKey(byte[] key, boolean exact) throws IOException {
        List<Integer> ancestorIndexes = new ArrayList<>();
        ancestorIndexes.add(0);  // index of root
        FindResult result = findKeyHelper(this, key, exact, ancestorIndexes);
        if (result == null) {
            return new FindResult(-1, null, ancestorIndexes);
        }
        return result;
    }

    public void findAll(List<Item> items) throws IOException {
        items.addAll(this.items);
        for (Long childNode: childNodes) {
            Node node = getNode(childNode);
            node.findAll(items);
        }
    }

    private FindResult findKeyHelper(Node node, byte[] key, boolean exact, List<Integer> ancestorsIndexes) throws IOException {
        Pair<Boolean, Integer> searchResult = node.findKeyInNode(key);
        Boolean wasFound = searchResult.key;
        int index = searchResult.value;

        if (wasFound) {
            return new FindResult(index, node, ancestorsIndexes);
        }

        if (node.isLeaf()) {
            if (exact) {
                return null;
            }
            return new FindResult(index, node, ancestorsIndexes);
        }

        ancestorsIndexes.add(index);
        Node nextChild = node.getNode(node.childNodes.get(index));
        return findKeyHelper(nextChild, key, exact, ancestorsIndexes);
    }


    public int elementSize(int i) {
        int size = 0;
        size += items.get(i).key().length;
        size += items.get(i).value().length;
        size += Constants.PageNumSize;

        return size;
    }

    public int nodeSize() {
        int size = 0;
        size += Constants.NodeHeaderSize;

        for (int i = 0; i < items.size(); i++) {
            size += elementSize(i);
        }

        // Add last page
        size += Constants.PageNumSize;
        return size;
    }

    public int addItem(Item item, int insertionIndex) {
        items.add(insertionIndex, item);
        return insertionIndex;
    }

    public boolean isOverPopulated() {
        return transaction.getDb().getDal().isOverPopulated(this);
    }

    public boolean isUnderPopulated() {
        return transaction.getDb().getDal().isUnderPopulated(this);
    }

    public void split(Node nodeToSplit, int nodeToSplitIndex) throws IOException {
        int splitIndex = nodeToSplit.transaction.getDb().getDal().getSplitIndex(nodeToSplit);

        Item middleItem = nodeToSplit.getItems().get(splitIndex);
        Node newNode;

        if (nodeToSplit.isLeaf()) {
            newNode = writeNode(transaction.newNode(
                    new ArrayList<>(nodeToSplit.getItems().subList(splitIndex + 1, nodeToSplit.getItems().size())), new ArrayList<>()));
            nodeToSplit.setItems(new ArrayList<>(nodeToSplit.getItems().subList(0, splitIndex)));
        } else {
            newNode = writeNode(transaction.newNode(new ArrayList<>(nodeToSplit.getItems().subList(splitIndex + 1, nodeToSplit.getItems().size())),
                    new ArrayList<>(nodeToSplit.getChildNodes().subList(splitIndex + 1, nodeToSplit.getChildNodes().size()))));
            nodeToSplit.setItems(new ArrayList<>(nodeToSplit.getItems().subList(0, splitIndex)));
            nodeToSplit.setChildNodes(new ArrayList<>(nodeToSplit.getChildNodes().subList(0, splitIndex + 1)));
        }

        addItem(middleItem, nodeToSplitIndex);
        if (childNodes.size() == nodeToSplitIndex + 1) {
            childNodes.add(newNode.getPageNum());
        } else {
            childNodes.add(nodeToSplitIndex + 1, newNode.getPageNum());
        }

        writeNodes(this, nodeToSplit);
    }

    public static class FindResult {
        private final int index;
        private final Node node;
        private final List<Integer> ancestorIndexes;

        public FindResult(int index, Node node, List<Integer> ancestorIndexes) {
            this.index = index;
            this.node = node;
            this.ancestorIndexes = ancestorIndexes;
        }

        public int getIndex() {
            return index;
        }

        public Node getNode() {
            return node;
        }

        public List<Integer> getAncestorIndexes() {
            return ancestorIndexes;
        }
    }

    public void removeItemFromLeaf(int index) throws IOException {
        items.remove(index);
        writeNode(this);
    }

    public List<Integer> removeItemFromInternal(int index) throws IOException {
        List<Integer> affectedNodes = new ArrayList<>();
        affectedNodes.add(index);

        Node aNode = getNode(childNodes.get(index));

        while (!aNode.isLeaf()) {
            // traversingIndex := len(n.childNodes) - 1 He used the current node's children here, not sure why
            // todo
            int traversingIndex = aNode.childNodes.size() - 1;
            aNode = aNode.getNode(aNode.childNodes.get(traversingIndex));
            affectedNodes.add(traversingIndex);
        }

        items.set(index, aNode.items.get(aNode.items.size() - 1));
        aNode.items.remove(aNode.items.size() - 1);
        writeNodes(this, aNode);

        return affectedNodes;
    }

    public void rotateRight(Node leftNode, Node parentNode, Node rightNode, int rightNodeIndex) {
        // Get last item and remove it
        Item leftNodeItem = leftNode.items.remove(leftNode.items.size() - 1);

        // Get item from parent node and assign the leftNode item instead
        int parentNodeItemIndex = rightNodeIndex - 1;
        if (isFirst(rightNodeIndex)) {
            parentNodeItemIndex = 0;
        }
        Item parentNodeItem = parentNode.items.get(parentNodeItemIndex);
        parentNode.items.set(parentNodeItemIndex, leftNodeItem);

        // Assign parent item to right and make it first
        rightNode.items.add(0, parentNodeItem);

        // If it's an inner node then move children as well.
        if (!leftNode.isLeaf()) {
            long childNodeToShift = leftNode.childNodes.remove(leftNode.childNodes.size() - 1);
            rightNode.childNodes.add(0, childNodeToShift);
        }
    }

    public void rotateLeft(Node leftNode, Node parentNode, Node rightNode, int rightNodeIndex) {
        // Get first item and remove it
        Item rightNodeItem = rightNode.items.remove(0);

        // Get item from parent node and assign the rightNodeItem item instead
        int parentNodeItemIndex = rightNodeIndex;
        if (isLast(rightNodeIndex)) {
            parentNodeItemIndex = parentNode.items.size() - 1;
        }
        Item parentNodeItem = parentNode.items.get(parentNodeItemIndex);
        parentNode.items.set(parentNodeItemIndex, rightNodeItem);

        // Assign parent item to a and make it last
        leftNode.items.add(parentNodeItem);

        // If it's an inner node then move children as well.
        if (!rightNode.isLeaf()) {
            long childNodeToShift = rightNode.childNodes.remove(0);
            leftNode.childNodes.add(childNodeToShift);
        }
    }

    public void merge(Node node, int nodeIndex) throws IOException {
        // Get the sibling node (aNode) to the left of bNode
        Node leftNode = getNode(childNodes.get(nodeIndex - 1));

        // Take the item from the parent, remove it, and add it to aNode
        Item parentNodeItem = items.remove(nodeIndex - 1);
        leftNode.items.add(parentNodeItem);

        // Merge items from bNode into aNode
        leftNode.items.addAll(node.items);

        // Remove bNode from the childNodes list
        childNodes.remove(nodeIndex);

        // If it's an inner node, merge child nodes as well
        if (!leftNode.isLeaf()) {
            leftNode.childNodes.addAll(node.childNodes);
        }

        // Write the nodes, delete bNode from persistent storage
        writeNodes(leftNode, this);
        transaction.getDb().getDal().deleteNode(node.pageNum);
    }

    public void rebalanceRemove(Node unbalancedNode, int unbalancedNodeIndex) throws IOException {
        Node parentNode = this;

        // Right rotate
        if (unbalancedNodeIndex != 0) {
            Node leftNode = getNode(parentNode.childNodes.get(unbalancedNodeIndex - 1));
            if (leftNode.canSpareAnElement()) {
                rotateRight(leftNode, parentNode, unbalancedNode, unbalancedNodeIndex);
                writeNodes(leftNode, parentNode, unbalancedNode);
                return;
            }
        }

        // Left Balance
        if (unbalancedNodeIndex != parentNode.childNodes.size() - 1) {
            Node rightNode = getNode(parentNode.childNodes.get(unbalancedNodeIndex + 1));
            if (rightNode.canSpareAnElement()) {
                rotateLeft(unbalancedNode, parentNode, rightNode, unbalancedNodeIndex);
                writeNodes(unbalancedNode, parentNode, rightNode);
                return;
            }
        }

        // Merge logic
        if (unbalancedNodeIndex == 0) {
            Node rightNode = getNode(childNodes.get(unbalancedNodeIndex + 1));
            parentNode.merge(rightNode, unbalancedNodeIndex + 1);
        } else {
            parentNode.merge(unbalancedNode, unbalancedNodeIndex);
        }
    }

    private boolean isFirst(int index) {
        return index == 0;
    }

    private boolean isLast(int index) {
        return index == items.size() - 1;
    }

    public boolean canSpareAnElement() {
        int splitIndex = transaction.getDb().getDal().getSplitIndex(this);
        return splitIndex != -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return pageNum == node.pageNum  && Objects.equals(items, node.items) && Objects.equals(childNodes, node.childNodes);
    }
}
