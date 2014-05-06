package storm.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import backtype.storm.tuple.Fields;

import com.google.common.collect.ImmutableMap;

public class StringKeyValueSchemeTest {

    private StringKeyValueScheme scheme = new StringKeyValueScheme();

    @Test
    public void testDeserialize() throws Exception {
        assertEquals(Arrays.asList("test"), scheme.deserialize("test".getBytes()));
    }

    @Test
    public void testGetOutputFields() throws Exception {
        Fields outputFields = scheme.getOutputFields();
        assertTrue(outputFields.contains(StringScheme.STRING_SCHEME_KEY));
        assertEquals(1, outputFields.size());
    }

    @Test
    public void testDeserializeWithNullKeyAndValue() throws Exception {
        assertEquals(Arrays.asList("test"), scheme.deserializeKeyAndValue(null, "test".getBytes()));
    }

    @Test
    public void testDeserializeWithKeyAndValue() throws Exception {
        assertEquals(Arrays.asList(ImmutableMap.of("key", "test")),
                scheme.deserializeKeyAndValue("key".getBytes(), "test".getBytes()));
    }
}
