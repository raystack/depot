package io.odpf.depot.config.converter;

import io.odpf.depot.common.TupleString;
import org.junit.Assert;
import org.junit.Test;

public class ConfToListConverterTest {

    @Test
    public void shouldReturnNullForEmpty() {
        ConfToListConverter converter = new ConfToListConverter();
        Assert.assertNull(converter.convert(null, ""));
    }

    @Test
    public void shouldCovertToTuple() {
        ConfToListConverter converter = new ConfToListConverter();
        Assert.assertEquals(new TupleString("a", "b"), converter.convert(null, "a=b"));
    }
}
