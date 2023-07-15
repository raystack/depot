package org.raystack.depot.message.field.proto;

import org.raystack.depot.message.field.GenericField;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DefaultFieldTest {

    @Test
    public void shouldReturnDefaultPrimitiveFields() {
        GenericField f = new DefaultField("test");
        Assert.assertEquals("test", f.getString());
        List<String> strings = new ArrayList<>();
        strings.add("test1");
        strings.add("test2");
        strings.add("test3");
        f = new DefaultField(strings);
        Assert.assertEquals("[\"test1\",\"test2\",\"test3\"]", f.getString());
        List<Integer> integers = new ArrayList<>();
        integers.add(123);
        integers.add(2323);
        integers.add(23);
        f = new DefaultField(integers);
        Assert.assertEquals("[123,2323,23]", f.getString());

        List<Instant> tss = new ArrayList<>();
        tss.add(Instant.ofEpochSecond(1000121010));
        tss.add(Instant.ofEpochSecond(1002121010));
        tss.add(Instant.ofEpochSecond(1003121010));
        f = new TimeStampField(tss);
        Assert.assertEquals("[\"2001-09-10T11:23:30Z\",\"2001-10-03T14:56:50Z\",\"2001-10-15T04:43:30Z\"]",
                f.getString());

        List<Boolean> booleanList = new ArrayList<>();
        booleanList.add(true);
        booleanList.add(false);
        booleanList.add(true);
        f = new DefaultField(booleanList);
        Assert.assertEquals("[true,false,true]", f.getString());

        List<Double> doubles = new ArrayList<>();
        doubles.add(123.93);
        doubles.add(13.0);
        doubles.add(23.0);
        f = new DefaultField(doubles);
        Assert.assertEquals("[123.93,13.0,23.0]", f.getString());

        List<TestEnum> enums = new ArrayList<>();
        enums.add(TestEnum.INACTIVE);
        enums.add(TestEnum.COMPLETED);
        enums.add(TestEnum.RUNNING);
        f = new DefaultField(enums);
        Assert.assertEquals("[\"INACTIVE\",\"COMPLETED\",\"RUNNING\"]", f.getString());

    }

    private enum TestEnum {
        COMPLETED,
        RUNNING,
        INACTIVE
    }
}
