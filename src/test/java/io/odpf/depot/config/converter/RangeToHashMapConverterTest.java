package io.odpf.depot.config.converter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

public class RangeToHashMapConverterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldConvertRangeToHashMap() {
        Map<Integer, Boolean> actualHashedRanges = new RangeToHashMapConverter().convert(null, "100-103");
        assertArrayEquals(new Integer[]{100, 101, 102, 103}, actualHashedRanges.keySet().toArray());
    }

    @Test
    public void shouldConvertRangesToHashMap() {
        Map<Integer, Boolean> actualHashedRanges = new RangeToHashMapConverter().convert(null, "100-103,200-203");
        assertArrayEquals(new Integer[]{100, 101, 102, 103, 200, 201, 202, 203}, actualHashedRanges.keySet().toArray());
    }

    @Test
    public void shouldThrowExceptionIfConfigNotRangeValues() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("input value '100' is not a valid range");
        new RangeToHashMapConverter().convert(null, "100,200-203");
    }

    @Test
    public void shouldThrowExceptionIfConfigIsInvalid() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("input value 'string' is not a valid range");
        new RangeToHashMapConverter().convert(null, "string");
    }
}
