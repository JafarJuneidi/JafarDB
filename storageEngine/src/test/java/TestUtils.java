import org.jafardb.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.UUID;

public class TestUtils {

    public static final int TEST_PAGE_SIZE = 4096;
    public static final float TEST_MIN_PERCENTAGE = 0.2F;
    public static final float TEST_MAX_PERCENTAGE = 0.55F;
    private static final int TEST_VAL_SIZE = 255;

    public static final int MOCK_NUMBER_OF_ELEMENTS = 10;
    private static final String EXPECTED_FOLDER_PATH = "../expected";

    public static final byte[] TEST_COLLECTION_NAME = "test1".getBytes();

    public static DB createTestDB() throws IOException, Constants.NotJafarDBFile {
        return DB.open(getTempFileName(), new Options(TEST_PAGE_SIZE, TEST_MIN_PERCENTAGE, TEST_MAX_PERCENTAGE));
    }

    public static void areCollectionsEqual(Collection c1, Collection c2) {
        assert Arrays.equals(c1.getName(), c2.getName());
        assert c1.getRoot() == c2.getRoot();
        assert c1.getCounter() == c2.getCounter();
    }

    public static void areTreesEqual(Collection t1, Collection t2) throws IOException {
        Node t1Root = t1.getTransaction().getNode(t1.getRoot());
        Node t2Root = t2.getTransaction().getNode(t2.getRoot());
        areTreesEqualHelper(t1Root, t2Root);
    }

    public static void areNodesEqual(Node n1, Node n2) {
        assert n1.getItems().size() == n2.getItems().size();
        for (int i = 0; i < n1.getItems().size(); i++) {
            assert Arrays.equals(n1.getItems().get(i).key(), n2.getItems().get(i).key());
            assert Arrays.equals(n1.getItems().get(i).value(), n2.getItems().get(i).value());
        }
    }

    public static void areTreesEqualHelper(Node n1, Node n2) throws IOException {
        assert n1.getItems().size() == n2.getItems().size();
        assert n1.getChildNodes().size() == n2.getChildNodes().size();
        areNodesEqual(n1, n2);
        for (int i = 0; i < n1.getChildNodes().size(); i++) {
            Node node1 = n1.getNode(n1.getChildNodes().get(i));
            Node node2 = n2.getNode(n2.getChildNodes().get(i));
            areTreesEqualHelper(node1, node2);
        }
    }

    public static Collection createTestMockTree() throws IOException, Constants.NotJafarDBFile, Constants.WriteInsideReadTransactionException {
        DB db = createTestDB();

        Transaction transaction = db.writeTransaction();
        List<Item> child0Items = createItems("0", "1");
        List<Item> child1Items = createItems("3", "4");
        List<Item> child2Items = createItems("6", "7", "8", "9");

        Node child0 = transaction.writeNode(transaction.newNode(child0Items, new ArrayList<>()));
        Node child1 = transaction.writeNode(transaction.newNode(child1Items, new ArrayList<>()));
        Node child2 = transaction.writeNode(transaction.newNode(child2Items, new ArrayList<>()));

        List<Item> rootItems = createItems("2", "5");
        Node root = transaction.writeNode(transaction.newNode(rootItems, Arrays.asList(child0.getPageNum(), child1.getPageNum(), child2.getPageNum())));

        Collection newCollection = new Collection();
        newCollection.setName(TEST_COLLECTION_NAME);
        newCollection.setRoot(root.getPageNum());
        Collection expectedCollection = transaction.createCollection(newCollection);
        transaction.commit();

        return expectedCollection;
    }

    public static String getExpectedResultFileName(String name) {
        return Paths.get(EXPECTED_FOLDER_PATH, name).toString();
    }

    public static String getTempFileName() {
        return Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString()).toString();
    }
    public static byte[] createItem(String key) {
        byte[] keyBytes = key.getBytes();
        byte[] keyBuf = new byte[TEST_VAL_SIZE];

        for (int i = 0; i < TEST_VAL_SIZE; i++) {
            keyBuf[i] = keyBytes[i % keyBytes.length];
        }

        return keyBuf;
    }

    public static List<Item> createItems(String... keys) {
        return Arrays.stream(keys)
                .map(TestUtils::createItem)
                .map(keyBuf -> new Item(keyBuf, keyBuf))
                .collect(Collectors.toList());
    }
}