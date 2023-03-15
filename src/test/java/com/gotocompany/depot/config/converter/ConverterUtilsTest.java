package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


public class ConverterUtilsTest {

    @Test
    public void testConvertEmpty() {
        List<Tuple<String, String>> tuples = ConverterUtils.convertToList("");
        Assert.assertEquals(0, tuples.size());
    }

    @Test
    public void testConvert() {
        List<Tuple<String, String>> tuples = ConverterUtils.convertToList("a=b,c=d");
        Assert.assertEquals(2, tuples.size());
        Assert.assertEquals(new Tuple<>("a", "b"), tuples.get(0));
        Assert.assertEquals(new Tuple<>("c", "d"), tuples.get(1));
    }
}
