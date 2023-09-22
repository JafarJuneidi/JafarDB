import org.jafar.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

public class CollectionTest {
    @Test
    void getAndCreateCollection() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection createdCollection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);
        transaction.commit();

        transaction = db.readTransaction();
        Collection actual = transaction.getCollection(createdCollection.getName()).get();
        transaction.commit();

        Collection expected = new Collection();
        expected.setRoot(createdCollection.getRoot());
        expected.setCounter(0);
        expected.setName(actual.getName());

        TestUtils.areCollectionsEqual(expected, actual);
    }

    @Test
    void getCollectionDoesntExist() throws IOException, Constants.NotJafarDBFile {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.readTransaction();
        Optional<Collection> collection = transaction.getCollection("name1".getBytes());
        assertTrue(collection.isEmpty());
    }

    @Test
    void createCollectionPutItem() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection createdCollection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);

        byte[] newKey = "0".getBytes();
        byte[] newVal = "1".getBytes();
        createdCollection.put(newKey, newVal);

        Optional<Item> item = createdCollection.find(newKey);

        assertTrue(item.isPresent());
        assertEquals(newKey, item.get().key());
        assertEquals(newVal, item.get().value());
    }

    @Test
    void deleteCollection() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection createdCollection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);
        transaction.commit();

        transaction = db.writeTransaction();
        Optional<Collection> actual = transaction.getCollection(createdCollection.getName());
        TestUtils.areCollectionsEqual(createdCollection, actual.get());

        transaction.deleteCollection(createdCollection.getName());

        Optional<Collection> actualAfterRemoval = transaction.getCollection(createdCollection.getName());
        assertTrue(actualAfterRemoval.isEmpty());

        transaction.commit();
    }

    @Test
    void deleteItem() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = TestUtils.createTestDB();

        Transaction transaction = db.writeTransaction();
        Collection createdCollection = transaction.createCollection(TestUtils.TEST_COLLECTION_NAME);

        byte[] newKey = "0".getBytes();
        byte[] newVal = "1".getBytes();
        createdCollection.put(newKey, newVal);

        Optional<Item> item = createdCollection.find(newKey);

        assertTrue(item.isPresent());
        assertEquals(newKey, item.get().key());
        assertEquals(newVal, item.get().value());

        createdCollection.remove(item.get().key());

        item = createdCollection.find(newKey);
        assertTrue(item.isEmpty());
    }

    @Test
    void serialize() throws IOException {
        byte[] expectedCollectionValue = Files.readAllBytes(Paths.get(TestUtils.getExpectedResultFileName("TestSerializeCollection")));

        Item expected = new Item("collection1".getBytes(), expectedCollectionValue);

        Collection collection = new Collection();
        collection.setName("collection1".getBytes());
        collection.setRoot(1);
        collection.setCounter(1);

        Item actual = collection.serialize();
        assertEquals(expected, actual);
    }

    @Test
    void deserialize() throws IOException {
        byte[] expectedCollectionValue = Files.readAllBytes(Paths.get(TestUtils.getExpectedResultFileName("TestSerializeCollection")));

        Collection expected = new Collection();
        expected.setName("collection1".getBytes());
        expected.setRoot(1);
        expected.setCounter(1);

        Item collection = new Item("collection1".getBytes(), expectedCollectionValue);

        Collection actual = new Collection();
        actual.deserialize(collection);

        TestUtils.areCollectionsEqual(expected, actual);
    }
}
