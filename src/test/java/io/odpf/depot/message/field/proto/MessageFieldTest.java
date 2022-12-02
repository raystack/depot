package io.odpf.depot.message.field.proto;

import io.odpf.depot.TestMessage;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class MessageFieldTest {

    @Test
    public void shouldReturnJsonStringForMessage() {
        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber("number")
                .setOrderDetails("details")
                .setOrderUrl("url")
                .build();
        MessageField field = new MessageField(message);
        Assert.assertEquals(
                new JSONObject("{\"order_number\":\"number\",\"order_url\":\"url\",\"order_details\":\"details\"}").toString(),
                new JSONObject(field.getString()).toString());
    }
}
