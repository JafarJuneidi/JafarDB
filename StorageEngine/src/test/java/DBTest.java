import org.jafar.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class DBTest {
    @Test
    void testDBCreateCollectionPutItem() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        Options options = new Options(TestUtils.TEST_PAGE_SIZE, 0.5F, 1.0F);
        DB db = DB.open(TestUtils.getTempFileName(), options);

        Transaction transaction = db.writeTransaction();
        Collection createdCollection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);

        byte[] newKey = "0".getBytes();
        byte[] newVal = "1".getBytes();

        createdCollection.put(newKey, newVal);
        Optional<Item> item = createdCollection.find(newKey);

        assertEquals(newKey, item.get().key());
        assertEquals(newVal, item.get().value());

        transaction.commit();
    }

    @Test
    void testDBWritersDontBlockReaders() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        Options options = new Options(TestUtils.TEST_PAGE_SIZE, 0.5F, 1.0F);
        DB db = DB.open(TestUtils.getTempFileName(), options);

        Transaction transaction = db.writeTransaction();
        Collection createdCollection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);

        byte[] newKey = "0".getBytes();
        byte[] newVal = "1".getBytes();

        createdCollection.put(newKey, newVal);
        Optional<Item> item = createdCollection.find(newKey);

        assertEquals(newKey, item.get().key());
        assertEquals(newVal, item.get().value());

        transaction.commit();

        Transaction holdingTransaction = db.writeTransaction();
        Transaction readTransaction = db.readTransaction();

        Collection collection = readTransaction.getCollection(createdCollection.getName()).get();
        TestUtils.areCollectionsEqual(createdCollection, collection);

        readTransaction.commit();
        holdingTransaction.commit();
    }

    @Test
    void testDBReadersDontSeeUncommittedChanges() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        Options options = new Options(TestUtils.TEST_PAGE_SIZE, 0.5F, 1.0F);
        DB db = DB.open(TestUtils.getTempFileName(), options);

        Transaction transaction1 = db.writeTransaction();
        Collection createdCollection = transaction1.createCollection(TestUtils.TEST_COLLECTION_NAME);
        transaction1.commit();

        Transaction transaction2 = db.writeTransaction();
        createdCollection = transaction2.getCollection(createdCollection.getName()).get();
        byte[] newKey = "0".getBytes();
        byte[] newVal = "1".getBytes();
        createdCollection.put(newKey, newVal);

        Transaction readTransaction = db.readTransaction();
        Collection collection = readTransaction.getCollection(createdCollection.getName()).get();
        TestUtils.areCollectionsEqual(createdCollection, collection);
        Optional<Item> item = collection.find(newKey);
        assertTrue(item.isEmpty());
        readTransaction.commit();

        transaction2.commit();
    }

    @Test
    void testDBDeleteItem() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        Options options = new Options(TestUtils.TEST_PAGE_SIZE, TestUtils.TEST_MIN_PERCENTAGE, TestUtils.TEST_MAX_PERCENTAGE);
        DB db = DB.open(TestUtils.getTempFileName(), options);

        Transaction transaction = db.writeTransaction();
        Collection createdCollection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);

        byte[] newKey = "0".getBytes();
        byte[] newVal = "1".getBytes();
        createdCollection.put(newKey, newVal);

        Optional<Item> item = createdCollection.find(newKey);

        assertTrue(item.isPresent());
        assertEquals(newKey, item.get().key());
        assertEquals(newVal, item.get().value());

        createdCollection.remove(newKey);

        item = createdCollection.find(newKey);

        assertTrue(item.isEmpty());

        transaction.commit();
    }
}
