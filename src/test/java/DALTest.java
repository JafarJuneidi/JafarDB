import org.jafardb.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DALTest {
    DAL createTestDAL() throws IOException, Constants.NotJafarDBFile {
        String fileName = TestUtils.getTempFileName();
        Options options = new Options();
        options.setPageSize(TestUtils.TEST_PAGE_SIZE);
        DAL dal = new DAL(fileName, options);

        return dal;
    }

    @Test
    void createAndGetNode() throws IOException, Constants.NotJafarDBFile {
        DAL dal = createTestDAL();
        List<Item> items = Arrays.asList(new Item("key1".getBytes(), "val1".getBytes()), new Item("key2".getBytes(), "val2".getBytes()));
        List<Long> childNodes = new ArrayList<>();

        Node expectedNode = dal.writeNode(new Node(items, childNodes));

        Node actualNode = dal.getNode(expectedNode.getPageNum());

        assertEquals(expectedNode, actualNode);
    }

    @Test
    void deleteNode() throws IOException, Constants.NotJafarDBFile {
        DAL dal = createTestDAL();
        List<Item> items = new ArrayList<>();
        List<Long> childNodes = new ArrayList<>();

        Node node = dal.writeNode(new Node(items, childNodes));
        assertEquals(node.getPageNum(), dal.getFreelist().getMaxPage());

        dal.deleteNode(node.getPageNum());

        assertArrayEquals(dal.getFreelist().getReleasedPages().toArray(), new Object[] {node.getPageNum()});
        assertEquals(node.getPageNum(), dal.getFreelist().getMaxPage());
    }

    @Test
    void deleteNodeAndReusePage() throws IOException, Constants.NotJafarDBFile {
        DAL dal = createTestDAL();
        List<Item> items = new ArrayList<>();
        List<Long> childNodes = new ArrayList<>();

        Node node = dal.writeNode(new Node(items, childNodes));
        assertEquals(node.getPageNum(), dal.getFreelist().getMaxPage());

        dal.deleteNode(node.getPageNum());

        assertArrayEquals(dal.getFreelist().getReleasedPages().toArray(), new Object[] {node.getPageNum()});
        assertEquals(node.getPageNum(), dal.getFreelist().getMaxPage());

        Node newNode = dal.writeNode(new Node(items, childNodes));
        assertArrayEquals(dal.getFreelist().getReleasedPages().toArray(), new Object[] {});
        assertEquals(newNode.getPageNum(), dal.getFreelist().getMaxPage());
    }

    @Test
    void createDalWithNewFile() throws IOException, Constants.NotJafarDBFile {
        DAL dal = createTestDAL();

        Meta metaPage = dal.readMeta();

        long freelistPageNum = 1L;
        long rootPageNum = 2L;

        assertEquals(freelistPageNum, metaPage.getFreelistPage());
        assertEquals(rootPageNum, metaPage.getRoot());
    }
}
