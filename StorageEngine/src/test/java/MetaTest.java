import org.jafar.Constants;
import org.jafar.Meta;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetaTest {

    @Test
    void testMetaSerialize() throws IOException {
        Meta meta = new Meta();
        meta.setRoot(3);
        meta.setFreelistPage(4);

        byte[] actual = new byte[TestUtils.TEST_PAGE_SIZE];
        meta.serialize(actual);

        byte[] expected = Files.readAllBytes(Paths.get(TestUtils.getExpectedResultFileName("TestMetaSerialize")));

        assertArrayEquals(expected, actual);
    }

    @Test
    void testMetaDeserialize() throws IOException, Constants.NotJafarDBFile {
        byte[] metaData = Files.readAllBytes(Paths.get(TestUtils.getExpectedResultFileName("TestMetaDeserialize")));

        Meta actual = new Meta();
        actual.deserialize(metaData);

        Meta expected = new Meta();
        expected.setRoot(3);
        expected.setFreelistPage(4);

        assertEquals(expected, actual);
    }
}
