package io.odpf.depot.message.field.proto;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DefaultFieldTest {

    @Test
    public void shouldReturnDefaultStringField() {
        DefaultField f = new DefaultField("test");
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
        TimeStampField tsf = new TimeStampField(tss);
        Assert.assertEquals("[\"2001-09-10T11:23:30Z\",\"2001-10-03T14:56:50Z\",\"2001-10-15T04:43:30Z\"]", tsf.getString());
    }
}
