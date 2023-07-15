package org.raystack.depot.message.proto;

import org.raystack.depot.TestMessage;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class UnknownProtoFieldsTest {

    @Test
    public void shouldGetEmptyStringWithWrongMessageBytes() {
        String out = UnknownProtoFields.toString("abcd".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("", out);
    }

    @Test
    public void shouldGetUnknownFields() {
        TestMessage message = TestMessage.newBuilder().setOrderDetails("test").build();
        String out = UnknownProtoFields.toString(message.toByteArray());
        Assert.assertEquals("3: \"test\"\n", out);
    }
}
