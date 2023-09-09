import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.jafardb.Freelist;
import org.jafardb.Page;
import org.junit.jupiter.api.Test;

class FreelistTest {
    @Test
    void testFreelistSerialize() throws Exception {
        Freelist freelist = new Freelist();
        freelist.setMaxPage(5);
        freelist.setReleasedPages(Arrays.asList(1L, 2L, 3L));  // Convert array to List<Long>
        byte[] actual = new byte[TestUtils.TEST_PAGE_SIZE];
        freelist.serialize(actual);

        byte[] expected = Files.readAllBytes(Paths.get(getExpectedResultFileName("TestFreelistSerialize")));

        assertArrayEquals(expected, actual);
    }

    @Test
    void testFreelistDeserialize() throws Exception {
        byte[] freelistData = Files.readAllBytes(Paths.get(getExpectedResultFileName("TestFreelistDeserialize")));

        Freelist actual = new Freelist();
        actual.deserialize(freelistData);

        Freelist expected = new Freelist();
        expected.setMaxPage(5);
        expected.setReleasedPages(Arrays.asList(1L, 2L, 3L));

        assertEquals(expected, actual);
    }

    private String getExpectedResultFileName(String testName) {
        return "expected/" + testName;
    }
}
