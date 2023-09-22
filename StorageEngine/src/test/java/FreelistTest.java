import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.jafar.*;
import org.junit.jupiter.api.Test;

class FreelistTest {
    @Test
    void testFreelistSerialize() throws Exception {
        Freelist freelist = new Freelist();
        freelist.setMaxPage(5);
        freelist.setReleasedPages(Arrays.asList(1L, 2L, 3L));  // Convert array to List<Long>
        byte[] actual = new byte[TestUtils.TEST_PAGE_SIZE];
        freelist.serialize(actual);

        byte[] expected = Files.readAllBytes(Paths.get(TestUtils.getExpectedResultFileName("TestFreelistSerialize")));

        assertArrayEquals(expected, actual);
    }

    @Test
    void testFreelistDeserialize() throws Exception {
        byte[] freelistData = Files.readAllBytes(Paths.get(TestUtils.getExpectedResultFileName("TestFreelistDeserialize")));

        Freelist actual = new Freelist();
        actual.deserialize(freelistData);

        Freelist expected = new Freelist();
        expected.setMaxPage(5);
        expected.setReleasedPages(Arrays.asList(1L, 2L, 3L));

        assertEquals(expected, actual);
    }
}
