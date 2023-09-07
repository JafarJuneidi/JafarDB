package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Collection {
    private byte[] name;
    private long root;

    private DAL dal;

    // Constructor
    public Collection(byte[] name, long root, DAL dal) {
        this.name = name;
        this.root = root;
        this.dal = dal;
    }

    public Item find(byte[] key) throws IOException {
        Node node = dal.getNode(root);
        var result = node.findKey(key, true);

        if (result.getIndex() == -1) {
            return null;
        }
        return result.getNode().getItems().get(result.getIndex());
    }

    public void put(byte[] key, byte[] value) throws IOException {
        Item item = new Item(key, value);

        // On first insertion, the root node does not exist, so it should be created
        Node root;
        if (this.root == 0) {
            root = dal.writeNode(dal.newNode(Arrays.asList(item), new ArrayList<>()));
            this.root = root.getPageNum();
            return;
        } else {
            root = dal.getNode(this.root);
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
        dal.writeNode(nodeToInsertIn);

        List<Node> ancestors = getNodes(ancestorsIndexes);

        // Rebalance the nodes all the way up. Start From one node before the last and go all the way up. Exclude root.
        for (int i = ancestors.size() - 2; i >= 0; i--) {
            Node pnode = ancestors.get(i);
            Node node = ancestors.get(i + 1);
            int nodeIndex = ancestorsIndexes.get(i + 1);
            if (node.isOverPopulated()) {
                pnode.split(node, nodeIndex);
            }
        }

        // Handle root
        Node rootNode = ancestors.get(0);
        if (rootNode.isOverPopulated()) {
            List<Long> list = new ArrayList<>();
            list.add(rootNode.getPageNum());
            Node newRoot = dal.newNode(new ArrayList<>(), list);
            newRoot.split(rootNode, 0);

            // Commit newly created root
            newRoot = dal.writeNode(newRoot);
            this.root = newRoot.getPageNum();
        }
    }

    public List<Node> getNodes(List<Integer> indexes) throws IOException {
        Node root = dal.getNode(this.root);
        if (root == null) {
            return null;  // Or you could throw an exception
        }

        List<Node> nodes = new ArrayList<>();
        nodes.add(root);

        Node child = root;
        for (int i = 1; i < indexes.size(); i++) {
            child = dal.getNode(child.getChildNodes().get(indexes.get(i)));
            if (child == null) {
                return null;  // Or you could throw an exception
            }
            nodes.add(child);
        }
        return nodes;
    }

    public boolean remove(byte[] key) throws Exception {
        // Find the path to the node where the deletion should happen
        Node rootNode = dal.getNode(root);
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
}
