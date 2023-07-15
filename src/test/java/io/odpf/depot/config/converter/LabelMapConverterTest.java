package org.raystack.depot.config.converter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

public class LabelMapConverterTest {

    @Test
    public void shouldConvertToEmptyMap() {
        LabelMapConverter converter = new LabelMapConverter();
        Assert.assertEquals(Collections.emptyMap(), converter.convert(null, ""));
    }

    @Test
    public void shouldConvertToMap() {
        LabelMapConverter converter = new LabelMapConverter();
        Assert.assertEquals(new HashMap<String, String>() {
            {
                put("a", "b");
                put("c", "d");
                put("test", "testing");
            }
        }, converter.convert(null, "a=b,c=d,test=testing"));
    }
}
