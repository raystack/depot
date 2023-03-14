package com.gotocompany.depot.message.field.proto;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestNestedRepeatedMessage;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class MessageFieldTest {

    @Test
    public void shouldReturnJsonStringForMessage() {
        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber("number")
                .setOrderDetails("details")
                .setOrderUrl("url")
                .build();
        MessageField field = new MessageField(message);
        String expectedJson = "{\"order_number\":\"number\",\"order_url\":\"url\",\"order_details\":\"details\"}";
        JSONAssert.assertEquals(expectedJson, field.getString(), true);
    }

    @Test
    public void shouldReturnMessageForRepeatedMessage() {
        TestNestedRepeatedMessage message = TestNestedRepeatedMessage.newBuilder()
                .addRepeatedMessage(TestMessage.newBuilder()
                        .setOrderNumber("number")
                        .setOrderDetails("details")
                        .setOrderUrl("url")
                        .build())
                .addRepeatedMessage(TestMessage.newBuilder()
                        .setOrderNumber("o2")
                        .setOrderDetails("d2")
                        .setOrderUrl("url2")
                        .build())
                .setSingleMessage(TestMessage.newBuilder()
                        .setOrderNumber("order1")
                        .setOrderDetails("de1")
                        .setOrderUrl("url1")
                        .build())
                .setNumberField(10)
                .addRepeatedNumberField(12)
                .addRepeatedNumberField(13)
                .setSingleTimestamp(Timestamp.newBuilder().setSeconds(1669962594).build())
                .addRepeatedTimestamp(Timestamp.newBuilder().setSeconds(1669932594).build())
                .addRepeatedTimestamp(Timestamp.newBuilder().setSeconds(1664932594).build())
                .build();
        MessageField field = new MessageField(message);
        String expectedJson = "{\n"
                + "  \"single_timestamp\": \"2022-12-02T06:29:54Z\",\n"
                + "  \"repeated_number_field\": [\n"
                + "    12,\n"
                + "    13\n"
                + "  ],\n"
                + "  \"repeated_timestamp\": [\n"
                + "    \"2022-12-01T22:09:54Z\",\n"
                + "    \"2022-10-05T01:16:34Z\"\n"
                + "  ],\n"
                + "  \"repeated_message\": [\n"
                + "    {\n"
                + "      \"order_url\": \"url\",\n"
                + "      \"order_number\": \"number\",\n"
                + "      \"order_details\": \"details\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"order_url\": \"url2\",\n"
                + "      \"order_number\": \"o2\",\n"
                + "      \"order_details\": \"d2\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"single_message\": {\n"
                + "    \"order_url\": \"url1\",\n"
                + "    \"order_number\": \"order1\",\n"
                + "    \"order_details\": \"de1\"\n"
                + "  },\n"
                + "  \"number_field\": 10\n"
                + "}\n";
        JSONAssert.assertEquals(expectedJson, field.getString(), true);

        expectedJson = "[\n"
                + "  {\n"
                + "    \"order_number\": \"number\",\n"
                + "    \"order_url\": \"url\",\n"
                + "    \"order_details\": \"details\"\n"
                + "  },\n"
                + "  {\n"
                + "    \"order_number\": \"o2\",\n"
                + "    \"order_url\": \"url2\",\n"
                + "    \"order_details\": \"d2\"\n"
                + "  }\n"
                + "]";
        field = new MessageField(message.getField(message.getDescriptorForType().findFieldByName("repeated_message")));
        JSONAssert.assertEquals(expectedJson, field.getString(), true);
    }
}
