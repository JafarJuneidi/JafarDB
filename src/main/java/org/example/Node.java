package org.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Node {
    private DAL dal;
    private long pageNum;
    private List<Item> items;
    private List<Long> childNodes;

    public Node() {}

    public void setPageNum(long pageNum) {
        this.pageNum = pageNum;
    }

    public long getPageNum() {
        return pageNum;
    }

    public boolean isLeaf() {
        return childNodes.isEmpty();
    }

    public byte[] serialize(byte[] buf) {
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

        return buf;
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
        return this.dal.writeNode(node);
    }

    public void writeNodes(Node... nodes) throws IOException {
        for (Node node : nodes) {
            this.writeNode(node);
        }
    }

    public Node getNode(long pageNum) throws IOException {
        return this.dal.getNode(pageNum);
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

    public record Pair<K, V>(K key, V value) {}

    public Pair<Integer, Node> findKey(byte[] key) throws IOException {
        Pair<Integer, Node> result = findKeyHelper(this, key);
        if (result == null) {
            return new Pair<>(-1, null);
        }
        return result;
    }

    private Pair<Integer, Node> findKeyHelper(Node node, byte[] key) throws IOException {
        Pair<Boolean, Integer> searchResult = node.findKeyInNode(key);
        Boolean wasFound = searchResult.key;
        int index = searchResult.value;

        if (wasFound) {
            return new Pair<>(index, node);
        }

        if (node.isLeaf()) {
            return null;
        }

        Node nextChild = node.getNode(node.childNodes.get(index));
        return findKeyHelper(nextChild, key);
    }
}
