import org.jafardb.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionTest {

    @Test
    void testTransactionCreateCollection() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();
        Transaction transaction = db.writeTransaction();
        Collection collection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);
        transaction.commit();

        transaction = db.readTransaction();
        Collection actualCollection = transaction.getCollection(collection.getName()).get();
        transaction.commit();

        TestUtils.areCollectionsEqual(collection, actualCollection);
    }

    @Test
    void testTransactionCreateCollectionReadTransaction() throws IOException, Constants.NotJafarDBFile {
        DB db = TestUtils.createTestDB();
        Transaction transaction = db.readTransaction();
        try {
            Collection collection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);
            fail("Should throw WriteInsideReadTransactionException");
        } catch (Constants.WriteInsideReadTransactionException ignored) {

        }
        transaction.commit();
    }

    @Test
    void testTransactionOpenMultipleReadTransactionSimultaneously() throws IOException, Constants.NotJafarDBFile {
        DB db = TestUtils.createTestDB();
        Transaction transaction1 = db.readTransaction();
        Transaction transaction2 = db.readTransaction();

        Optional<Collection> collection1 = transaction1.getCollection(TestUtils.TEST_COLLECTION_NAME);
        assertTrue(collection1.isEmpty());

        Optional<Collection> collection2 = transaction2.getCollection(TestUtils.TEST_COLLECTION_NAME);
        assertTrue(collection2.isEmpty());

        transaction1.commit();
        transaction2.commit();
    }

    @Test
    void testTransactionOpenReadAndWriteSimultaneously() throws IOException, Constants.NotJafarDBFile, InterruptedException {
        DB db = TestUtils.createTestDB();
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        // Assuming that acquiring a read lock is synchronous, so we initiate the first read transaction.
        Transaction transaction1 = db.readTransaction();

        // Start a write transaction in a separate thread.
        Thread t1 = new Thread(() -> {
            Transaction transaction2 = db.writeTransaction();

            // Start another read transaction in yet another thread.
            Thread t2 = new Thread(() -> {
                Transaction transaction3 = db.readTransaction();

                Optional<Collection> collection3 = null;
                try {
                    collection3 = transaction3.getCollection(TestUtils.TEST_COLLECTION_NAME);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                assertArrayEquals(TestUtils.TEST_COLLECTION_NAME, collection3.get().getName());
                try {
                    transaction3.commit();
                } catch (Exception e) {
                    fail("Unexpected error: " + e.getMessage());
                }

                latch2.countDown();
            });
            t2.start();

            try {
                Collection collection = transaction2.createCollection(TestUtils.TEST_COLLECTION_NAME);
            } catch (Exception e) {
                fail("Unexpected error: " + e.getMessage());
            }

            try {
                transaction2.commit();
            } catch (Exception e) {
                fail("Unexpected error: " + e.getMessage());
            }

            latch1.countDown();
        });
        t1.start();

        Optional<Collection> collection1 = null;
        try {
            collection1 = transaction1.getCollection(TestUtils.TEST_COLLECTION_NAME);
        } catch (Exception e) {
            fail("Unexpected error: " + e.getMessage());
        }

        assertTrue(collection1.isEmpty());

        try {
            transaction1.commit();
        } catch (Exception e) {
            fail("Unexpected error: " + e.getMessage());
        }

        latch1.await();
        latch2.await();
    }

    @Test
    void testTransactionRollback() throws Constants.WriteInsideReadTransactionException, IOException, Constants.NotJafarDBFile {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Node child0 = transaction.writeNode(transaction.newNode(TestUtils.createItems("0", "1", "2", "3"), new ArrayList<>()));
        Node child1 = transaction.writeNode(transaction.newNode(TestUtils.createItems("5", "6", "7", "8"), new ArrayList<>()));
        Node root = transaction.writeNode(transaction.newNode(TestUtils.createItems("4"), Arrays.asList(child0.getPageNum(), child1.getPageNum())));

        Collection collection = new Collection();
        collection.setName(TestUtils.TEST_COLLECTION_NAME);
        collection.setRoot(root.getPageNum());

        collection = transaction.createCollection(collection);
        transaction.commit();

        assertEquals(transaction.getDb().getDal().getFreelist().getReleasedPages().size(), 0);

        Transaction transaction2 = db.writeTransaction();
        collection = transaction2.getCollection(collection.getName()).get();
        byte[] val = TestUtils.createItem("9");
        collection.put(val, val);
        transaction2.rollback();
        assertEquals(transaction2.getDb().getDal().getFreelist().getReleasedPages().size(), 1);

        Transaction transaction3 = db.readTransaction();
        collection = transaction3.getCollection(collection.getName()).get();
        byte[] expectedVal = TestUtils.createItem("9");
        Optional<Item> item = collection.find(expectedVal);
        assertTrue(item.isEmpty());
        transaction3.commit();

        assertEquals(transaction2.getDb().getDal().getFreelist().getReleasedPages().size(), 1);
    }
}