import org.jafardb.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NodeTest {
    @Test
    void addSingle() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection collection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);
        byte[] value = TestUtils.createItem("0");
        collection.put(value, value);
        transaction.commit();

        DB expectedDb = TestUtils.createTestDB();

        Transaction expectedTransaction = expectedDb.writeTransaction();
        Node expectedRoot = expectedTransaction.writeNode(expectedTransaction.newNode(TestUtils.createItems("0"), new ArrayList<>()));

        Collection expectedCollection = expectedTransaction.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, expectedRoot.getPageNum()));
        expectedTransaction.commit();

        TestUtils.areTreesEqual(expectedCollection, collection);
    }

    @Test
    void removeFromRootSingleElement() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection collection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);
        byte[] value = TestUtils.createItem("0");
        collection.put(value, value);
        collection.remove(value);
        transaction.commit();

        DB expectedDbAfterRemoval = TestUtils.createTestDB();
        Transaction expectedTransactionAfterRemoval = expectedDbAfterRemoval.writeTransaction();
        Node expectedRootAfterRemoval = expectedTransactionAfterRemoval.writeNode(expectedTransactionAfterRemoval.newNode(new ArrayList<>(), new ArrayList<>()));
        Collection expectedCollectionAfterRemoval = expectedTransactionAfterRemoval.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, expectedRootAfterRemoval.getPageNum()));
        expectedTransactionAfterRemoval.commit();

        TestUtils.areTreesEqual(expectedCollectionAfterRemoval, collection);
    }

    @Test
    void addMultiple() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection collection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);

        int numOfElements = TestUtils.MOCK_NUMBER_OF_ELEMENTS;
        for (int i = 0; i < numOfElements; ++i) {
            byte[] val = TestUtils.createItem(Integer.toString(i));
            collection.put(val, val);
        }
        transaction.commit();

        Collection expected = TestUtils.createTestMockTree();
        TestUtils.areTreesEqual(expected, collection);
    }

    @Test
    void addAndRebalanceSplit() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();
        Transaction transaction = db.writeTransaction();
        Node child0 = transaction.writeNode(transaction.newNode(TestUtils.createItems("0", "1", "2", "3"), new ArrayList<>()));
        Node child1 = transaction.writeNode(transaction.newNode(TestUtils.createItems("5", "6", "7", "8"), new ArrayList<>()));
        Node root = transaction.writeNode(transaction.newNode(TestUtils.createItems("4"), new ArrayList<>(Arrays.asList(child0.getPageNum(), child1.getPageNum()))));

        Collection collection = transaction.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, root.getPageNum()));
        byte[] val = TestUtils.createItem("9");
        collection.put(val, val);
        transaction.commit();

        DB expectedTestDb = TestUtils.createTestDB();
        Transaction testTransaction = expectedTestDb.writeTransaction();
        Node expectedChild0 = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("0", "1", "2", "3"), new ArrayList<>()));
        Node expectedChild1 = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("5", "6"), new ArrayList<>()));
        Node expectedChild2 = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("8", "9"), new ArrayList<>()));
        Node expectedRoot = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("4", "7"),
                new ArrayList<>(Arrays.asList(expectedChild0.getPageNum(), expectedChild1.getPageNum(), expectedChild2.getPageNum()))));

        Collection expectedCollection = testTransaction.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, expectedRoot.getPageNum()));
        testTransaction.commit();

        TestUtils.areTreesEqual(expectedCollection, collection);
    }

    @Test
    void splitAndMerge() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();
        Transaction transaction = db.writeTransaction();
        Node child0 = transaction.writeNode(transaction.newNode(TestUtils.createItems("0", "1", "2", "3"), new ArrayList<>()));
        Node child1 = transaction.writeNode(transaction.newNode(TestUtils.createItems("5", "6", "7", "8"), new ArrayList<>()));
        Node root = transaction.writeNode(transaction.newNode(TestUtils.createItems("4"), new ArrayList<>(Arrays.asList(child0.getPageNum(), child1.getPageNum()))));

        Collection collection = transaction.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, root.getPageNum()));
        byte[] val = TestUtils.createItem("9");
        collection.put(val, val);
        transaction.commit();

        DB expectedTestDb = TestUtils.createTestDB();
        Transaction testTransaction = expectedTestDb.writeTransaction();
        Node expectedChild0 = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("0", "1", "2", "3"), new ArrayList<>()));
        Node expectedChild1 = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("5", "6"), new ArrayList<>()));
        Node expectedChild2 = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("8", "9"), new ArrayList<>()));
        Node expectedRoot = testTransaction.writeNode(testTransaction.newNode(TestUtils.createItems("4", "7"),
                new ArrayList<>(Arrays.asList(expectedChild0.getPageNum(), expectedChild1.getPageNum(), expectedChild2.getPageNum()))));

        Collection expectedCollection = testTransaction.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, expectedRoot.getPageNum()));

        TestUtils.areTreesEqual(expectedCollection, collection);
        testTransaction.commit();

        Transaction removeTransaction = db.writeTransaction();
        collection = removeTransaction.getCollection(collection.getName()).get();
        collection.remove(val);
        removeTransaction.commit();


        DB expectedDbAfterRemoval = TestUtils.createTestDB();
        Transaction expectedTransactionAfterRemoval = expectedDbAfterRemoval.writeTransaction();
        Node expectedChild0AfterRemoval = expectedTransactionAfterRemoval.writeNode(expectedTransactionAfterRemoval.newNode(TestUtils.createItems("0", "1", "2", "3"), new ArrayList<>()));
        Node expectedChild1AfterRemoval = expectedTransactionAfterRemoval.writeNode(expectedTransactionAfterRemoval.newNode(TestUtils.createItems("5", "6", "7", "8"), new ArrayList<>()));
        Node expectedRootAfterRemoval = expectedTransactionAfterRemoval.writeNode(expectedTransactionAfterRemoval.newNode(
                TestUtils.createItems("4"),
                new ArrayList<>(Arrays.asList(expectedChild0AfterRemoval.getPageNum(), expectedChild1AfterRemoval.getPageNum()))));

        Collection expectedCollectionAfterRemoval = expectedTransactionAfterRemoval.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, expectedRootAfterRemoval.getPageNum()));
        expectedTransactionAfterRemoval.commit();
        TestUtils.areTreesEqual(expectedCollectionAfterRemoval, collection);
    }

    @Test
    void removeFromRootWithoutRebalance() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection collection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);

        int numOfElements = TestUtils.MOCK_NUMBER_OF_ELEMENTS;
        for (int i = 0; i < numOfElements; ++i) {
            byte[] val = TestUtils.createItem(Integer.toString(i));
            collection.put(val, val);
        }
        collection.remove(TestUtils.createItem("7"));
        transaction.commit();

        DB expectedDb = TestUtils.createTestDB();
        Transaction expectedTestTransaction = expectedDb.writeTransaction();

        Node expectedChild0 = expectedTestTransaction.writeNode(expectedTestTransaction.newNode(TestUtils.createItems("0", "1"), new ArrayList<>()));
        Node expectedChild1 = expectedTestTransaction.writeNode(expectedTestTransaction.newNode(TestUtils.createItems("3", "4"), new ArrayList<>()));
        Node expectedChild2 = expectedTestTransaction.writeNode(expectedTestTransaction.newNode(TestUtils.createItems("6", "8", "9"), new ArrayList<>()));
        Node expectedRoot = expectedTestTransaction.writeNode(expectedTestTransaction.newNode(
                TestUtils.createItems("2", "5"),
                new ArrayList<>(Arrays.asList(expectedChild0.getPageNum(), expectedChild1.getPageNum(), expectedChild2.getPageNum()))));

        Collection expectedCollectionAfterRemoval = expectedTestTransaction.createCollection(new Collection(TestUtils.TEST_COLLECTION_NAME, expectedRoot.getPageNum()));
        expectedTestTransaction.commit();

        TestUtils.areTreesEqual(expectedCollectionAfterRemoval, collection);
    }
}
