package io.odpf.depot.message.proto;

import com.google.api.client.util.DateTime;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.odpf.depot.StatusBQ;
import io.odpf.depot.TestKeyBQ;
import io.odpf.depot.TestMessageBQ;
import io.odpf.depot.TestNestedMessageBQ;
import io.odpf.depot.TestNestedRepeatedMessageBQ;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.stencil.Parser;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ProtoOdpfParsedMessageTest {

    private Timestamp createdAt;
    private DynamicMessage dynamicMessage;
    private Instant now;
    private long nowMillis;
    private ProtoOdpfMessageParser odpfMessageParser;
    @Mock
    private StencilClient stencilClient;
    private Map<String, Descriptors.Descriptor> descriptorsMap;

    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        stencilClient = Mockito.mock(StencilClient.class);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        now = Instant.now();
        createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        TestMessageBQ testMessage = TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url")
                .setOrderDetails("order-details")
                .setCreatedAt(createdAt)
                .setStatus(StatusBQ.COMPLETED)
                .setOrderDate(com.google.type.Date.newBuilder().setYear(1996).setMonth(11).setDay(21))
                .build();

        dynamicMessage = protoParser.parse(testMessage.toByteArray());
        nowMillis = Instant.ofEpochSecond(now.getEpochSecond(), now.getNano()).toEpochMilli();
        descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
            put(String.format("%s", TestMessageBQ.class.getName()), TestMessageBQ.getDescriptor());
            put(String.format("%s", TestNestedMessageBQ.class.getName()), TestNestedMessageBQ.getDescriptor());
            put(String.format("%s", TestNestedRepeatedMessageBQ.class.getName()), TestNestedRepeatedMessageBQ.getDescriptor());
            put("io.odpf.depot.TestMessageBQ.CurrentStateEntry", TestMessageBQ.getDescriptor().getNestedTypes().get(0));
            put("com.google.protobuf.Struct.FieldsEntry", Struct.getDescriptor().getNestedTypes().get(0));
            put("com.google.protobuf.Duration", com.google.protobuf.Duration.getDescriptor());
            put("com.google.type.Date", com.google.type.Date.getDescriptor());
        }};
        odpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
    }

    @Test
    public void shouldReturnFieldsInProperties() throws IOException {
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(dynamicMessage).getMapping(odpfMessageSchema);
        assertEquals("order-1", fields.get("order_number"));
        assertEquals("order-url", fields.get("order_url"));
        assertEquals("order-details", fields.get("order_details"));
        assertEquals(new DateTime(nowMillis), fields.get("created_at"));
        assertEquals("COMPLETED", fields.get("status"));
        Map dateFields = (Map) fields.get("order_date");
        assertEquals(1996, dateFields.get("year"));
        assertEquals(11, dateFields.get("month"));
        assertEquals(21, dateFields.get("day"));
    }

    @Test
    public void shouldParseDurationMessageSuccessfully() throws IOException {
        TestMessageBQ message = TestProtoUtil.generateTestMessage(now);
        Parser messageProtoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(messageProtoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);
        Map durationFields = (Map) fields.get("trip_duration");
        assertEquals("order-1", fields.get("order_number"));
        assertEquals((long) 1, durationFields.get("seconds"));
        assertEquals(1000000000, durationFields.get("nanos"));
    }

    @Test
    public void shouldParseNestedMessageSuccessfully() {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ message2 = TestProtoUtil.generateTestMessage(now);

        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage1 = TestProtoUtil.generateTestNestedMessage("nested-message-1", message1);
        TestNestedMessageBQ nestedMessage2 = TestProtoUtil.generateTestNestedMessage("nested-message-2", message2);
        Arrays.asList(nestedMessage1, nestedMessage2).forEach(msg -> {
            Map<String, Object> fields = null;
            try {
                fields = new ProtoOdpfParsedMessage(protoParser.parse(msg.toByteArray()))
                        .getMapping(odpfMessageParser.getSchema("io.odpf.depot.TestNestedMessageBQ", descriptorsMap));
            } catch (IOException e) {
                e.printStackTrace();
            }
            assertNestedMessage(msg, fields);
        });
    }

    @Test
    public void shouldParseRepeatedPrimitives() throws IOException {
        String orderNumber = "order-1";
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl("order-url-1")
                .addAliases("alias1").addAliases("alias2")
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);

        assertEquals(orderNumber, fields.get("order_number"));
        assertEquals(Arrays.asList("alias1", "alias2"), fields.get("aliases"));
    }

    @Test
    public void shouldParseRepeatedNestedMessages() throws IOException {
        int number = 1234;
        TestMessageBQ nested1 = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ nested2 = TestProtoUtil.generateTestMessage(now);
        TestNestedRepeatedMessageBQ message = TestNestedRepeatedMessageBQ.newBuilder()
                .setNumberField(number)
                .addRepeatedMessage(nested1)
                .addRepeatedMessage(nested2)
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedRepeatedMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestNestedRepeatedMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);

        assertEquals(number, fields.get("number_field"));
        List repeatedMessagesMap = (List) fields.get("repeated_message");
        assertTestMessageFields((Map) repeatedMessagesMap.get(0), nested1);
        assertTestMessageFields((Map) repeatedMessagesMap.get(1), nested2);
    }

    @Test
    public void shouldParseRepeatedNestedMessagesIfRepeatedFieldsAreMissing() throws IOException {
        int number = 1234;
        TestNestedRepeatedMessageBQ message = TestNestedRepeatedMessageBQ.newBuilder()
                .setNumberField(number)
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedRepeatedMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestNestedRepeatedMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);

        assertEquals(number, fields.get("number_field"));
        assertEquals(1, fields.size());
    }

    @Test
    public void shouldParseMapFields() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url-1")
                .setOrderDetails("order-details-1")
                .putCurrentState("state_key_1", "state_value_1")
                .putCurrentState("state_key_2", "state_value_2")
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);

        assertEquals(message.getOrderNumber(), fields.get("order_number"));
        assertEquals(message.getOrderUrl(), fields.get("order_url"));
        List repeatedStateMap = (List) fields.get("current_state");
        assertEquals("state_key_1", ((Map) repeatedStateMap.get(0)).get("key"));
        assertEquals("state_value_1", ((Map) repeatedStateMap.get(0)).get("value"));
        assertEquals("state_key_2", ((Map) repeatedStateMap.get(1)).get("key"));
        assertEquals("state_value_2", ((Map) repeatedStateMap.get(1)).get("value"));
    }

    @Test
    public void shouldMapStructFields() throws IOException {
        ListValue.Builder builder = ListValue.newBuilder();
        ListValue listValue = builder
                .addValues(Value.newBuilder().setNumberValue(1).build())
                .addValues(Value.newBuilder().setNumberValue(2).build())
                .addValues(Value.newBuilder().setNumberValue(3).build())
                .build();
        Struct value = Struct.newBuilder()
                .putFields("number", Value.newBuilder().setNumberValue(123.45).build())
                .putFields("string", Value.newBuilder().setStringValue("string_val").build())
                .putFields("list", Value.newBuilder().setListValue(listValue).build())
                .putFields("boolean", Value.newBuilder().setBoolValue(true).build())
                .build();

        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setProperties(value)
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);


        assertEquals(message.getOrderNumber(), fields.get("order_number"));
        String expectedProperties = "{\"number\":123.45,\"string\":\"string_val\",\"list\":[1.0,2.0,3.0],\"boolean\":true}";
        assertEquals(expectedProperties, fields.get("properties"));
    }

    private void assertNestedMessage(TestNestedMessageBQ msg, Map<String, Object> fields) {
        assertEquals(msg.getNestedId(), fields.get("nested_id"));
        Map nestedFields = (Map) fields.get("single_message");
        assertNotNull(nestedFields);
        TestMessageBQ message = msg.getSingleMessage();
        assertTestMessageFields(nestedFields, message);
    }

    private void assertTestMessageFields(Map nestedFields, TestMessageBQ message) {
        assertEquals(message.getOrderNumber(), nestedFields.get("order_number"));
        assertEquals(message.getOrderUrl(), nestedFields.get("order_url"));
        assertEquals(message.getOrderDetails(), nestedFields.get("order_details"));
        assertEquals(new DateTime(nowMillis), nestedFields.get("created_at"));
        Assert.assertEquals(message.getStatus().toString(), nestedFields.get("status"));
    }

    @Test()
    public void shouldReturnNullWhenIndexNotPresent() throws IOException {
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestNestedRepeatedMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(dynamicMessage).getMapping(odpfMessageSchema);

        assertNull(fields.get("single_message"));
    }

    @Test
    public void shouldReturnNullWhenNoDateFieldIsProvided() throws IOException {
        TestMessageBQ testMessage = TestMessageBQ.newBuilder()
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        dynamicMessage = protoParser.parse(testMessage.toByteArray());

        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(dynamicMessage).getMapping(odpfMessageSchema);

        assertNull(fields.get("order_date"));
    }

    @Test
    public void shouldParseRepeatedTimestamp() throws IOException {
        createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();

        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addUpdatedAt(createdAt).addUpdatedAt(createdAt)
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());

        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);

        assertEquals(Arrays.asList(new DateTime(now.toEpochMilli()), new DateTime(now.toEpochMilli())), fields.get("updated_at"));
    }

    @Test
    public void shouldParseStructField() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setProperties(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("50").build()).build())
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());

        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);

        assertEquals("{\"name\":\"John\",\"age\":\"50\"}", fields.get("properties"));
    }

    @Test
    public void shouldParseRepeatableStructField() throws IOException {
        Value val = Value.newBuilder().setStringValue("test").build();

        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("50").build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("60").build()).build())
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray())).getMapping(odpfMessageSchema);

        assertEquals(Arrays.asList("{\"name\":\"John\",\"age\":\"50\"}", "{\"name\":\"John\",\"age\":\"60\"}"), fields.get("attributes"));
    }
}
