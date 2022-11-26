package io.odpf.depot.message.proto;

import com.google.api.client.util.DateTime;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import io.odpf.depot.*;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.stencil.Parser;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ProtoOdpfParsedMessageTest {

    private final Configuration configuration = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();
    private final JsonFormat.Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .includingDefaultValueFields();
    private Timestamp createdAt;
    private DynamicMessage dynamicMessage;
    private Instant now;
    private long nowMillis;
    private ProtoOdpfMessageParser odpfMessageParser;
    private Parser parser;
    @Mock
    private StencilClient stencilClient;
    private Map<String, Descriptors.Descriptor> descriptorsMap;

    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        stencilClient = Mockito.mock(StencilClient.class);
        parser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
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

        dynamicMessage = parser.parse(testMessage.toByteArray());
        nowMillis = Instant.ofEpochSecond(now.getEpochSecond(), now.getNano()).toEpochMilli();
        descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
            put(String.format("%s", TestMessageBQ.class.getName()), TestMessageBQ.getDescriptor());
            put(String.format("%s", TestNestedMessageBQ.class.getName()), TestNestedMessageBQ.getDescriptor());
            put(String.format("%s", TestNestedRepeatedMessageBQ.class.getName()), TestNestedRepeatedMessageBQ.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.TopicMetadata.class.getName()), TestBookingLogMessage.TopicMetadata.getDescriptor());
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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(dynamicMessage, null, null).getMapping(odpfMessageSchema);
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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(messageProtoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);
        Map durationFields = (Map) fields.get("trip_duration");
        assertEquals("order-1", fields.get("order_number"));
        assertEquals((long) 1, durationFields.get("seconds"));
        assertEquals(TestProtoUtil.TRIP_DURATION_NANOS, durationFields.get("nanos"));
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
                fields = new ProtoOdpfParsedMessage(protoParser.parse(msg.toByteArray()), null, null)
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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);

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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);

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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);

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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);

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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);


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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(dynamicMessage, null, null).getMapping(odpfMessageSchema);

        assertNull(fields.get("single_message"));
    }

    @Test
    public void shouldReturnNullWhenNoDateFieldIsProvided() throws IOException {
        TestMessageBQ testMessage = TestMessageBQ.newBuilder()
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        dynamicMessage = protoParser.parse(testMessage.toByteArray());

        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(dynamicMessage, null, null).getMapping(odpfMessageSchema);

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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);

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
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);

        assertEquals("{\"name\":\"John\",\"age\":\"50\"}", fields.get("properties"));
    }

    @Test
    public void shouldParseRepeatableStructField() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("50").build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("60").build()).build())
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Map<String, Object> fields = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null).getMapping(odpfMessageSchema);
        assertEquals(Arrays.asList("{\"name\":\"John\",\"age\":\"50\"}", "{\"name\":\"John\",\"age\":\"60\"}"), fields.get("attributes"));
    }

    @Test
    public void shouldCacheMappingForSameSchema() throws IOException {
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("50").build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("60").build()).build())
                .build();
        OdpfMessageSchema odpfMessageSchema1 = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        OdpfMessageSchema odpfMessageSchema2 = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ParsedOdpfMessage parsedOdpfMessage = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), null, null);
        Map<String, Object> map1 = parsedOdpfMessage.getMapping(odpfMessageSchema1);
        Map<String, Object> map2 = parsedOdpfMessage.getMapping(odpfMessageSchema2);
        assertEquals(map1, map2);
    }


    @Test
    public void shouldGetFieldByName() throws IOException {
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(dynamicMessage, configuration, jsonPrinter);
        Assert.assertEquals("order-1", protoOdpfParsedMessage.getFieldByName("order_number", odpfMessageSchema));
    }

    @Test
    public void shouldGetComplexFieldByName() throws IOException {
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestBookingLogMessage", descriptorsMap);
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        DynamicMessage bookingLogDynamicMessage = protoParser.parse(testBookingLogMessage.toByteArray());
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(bookingLogDynamicMessage, configuration, jsonPrinter);
        Assert.assertEquals("[{\"qos\":1,\"topic\":\"hellowo/rl/dcom.world.partner\"}]", protoOdpfParsedMessage.getFieldByName("topics", odpfMessageSchema).toString());
    }

    @Test
    public void shouldGetStructFromProto() throws IOException {
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestBookingLogMessage", descriptorsMap);
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(10.0).setLongitude(12.0).build())
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        DynamicMessage bookingLogDynamicMessage = protoParser.parse(testBookingLogMessage.toByteArray());
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(bookingLogDynamicMessage, configuration, jsonPrinter);
        Assert.assertEquals("{\"note\":\"\",\"accuracy_meter\":0,\"address\":\"\",\"gate_id\":\"\",\"latitude\":10,\"name\":\"\",\"type\":\"\",\"place_id\":\"\",\"longitude\":12}",
                protoOdpfParsedMessage.getFieldByName("driver_pickup_location", odpfMessageSchema).toString());
    }

    @Test
    public void shouldGetRepeatableStructField() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(60).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("active", Value.newBuilder().setBoolValue(true).build())
                        .putFields("height", Value.newBuilder().setNumberValue(175).build()).build())
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        String attributes = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), configuration, jsonPrinter).getFieldByName("attributes", odpfMessageSchema).toString();
        Assert.assertEquals("[{\"name\":\"John\",\"age\":50},{\"name\":\"John\",\"age\":60},{\"name\":\"John\",\"active\":true,\"height\":175}]", attributes);
    }

    @Test
    public void shouldGetNumberFields() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50L).build()).build())
                .setDiscount(10000012010L)
                .setPrice(10.2f)
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(protoParser.parse(message.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("10000012010", protoOdpfParsedMessage.getFieldByName("discount", odpfMessageSchema));
        Assert.assertEquals(10.2d, ((BigDecimal) protoOdpfParsedMessage.getFieldByName("price", odpfMessageSchema)).doubleValue(), 0.00000000001);
    }

    @Test
    public void shouldGetFieldByNameFromNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestNestedMessageBQ", descriptorsMap);
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(protoParser.parse(nestedMessage.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("test", protoOdpfParsedMessage.getFieldByName("nested_id", odpfMessageSchema));
        Assert.assertEquals(message1.getOrderNumber(), protoOdpfParsedMessage.getFieldByName("single_message.order_number", odpfMessageSchema));
    }

    @Test
    public void shouldReturnTimestampFieldInRFC3339Format() throws IOException {
        Instant time = Instant.ofEpochSecond(1669160207, 600000000);
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(time);
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(parser.parse(message1.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("2022-11-22T23:36:47.600Z", protoOdpfParsedMessage.getFieldByName("created_at", odpfMessageSchema));
    }

    @Test
    public void shouldReturnDurationFieldInStringFormat() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(parser.parse(message1.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("1.000001s", protoOdpfParsedMessage.getFieldByName("trip_duration", odpfMessageSchema));
    }

    @Test
    public void shouldReturnMapFieldAsJSONObject() throws IOException {
        TestMessageBQ message1 = TestMessageBQ.newBuilder().putCurrentState("key", "value").build();
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(parser.parse(message1.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("{\"key\":\"value\"}", protoOdpfParsedMessage.getFieldByName("current_state", odpfMessageSchema).toString());
    }

    @Test
    public void shouldReturnDefaultValueForFieldIfValueIsNotSet() throws IOException {
        TestMessageBQ emptyMessage = TestMessageBQ.newBuilder().build();
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(parser.parse(emptyMessage.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("", protoOdpfParsedMessage.getFieldByName("order_number", odpfMessageSchema));
    }

    @Test
    public void shouldThrowExceptionIfColumnIsNotPresentInProto() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestNestedMessageBQ", descriptorsMap);
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(protoParser.parse(nestedMessage.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("test", protoOdpfParsedMessage.getFieldByName("nested_id", odpfMessageSchema));
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoOdpfParsedMessage.getFieldByName("single_message.order_id", odpfMessageSchema));
        Assert.assertEquals("Invalid field config : single_message.order_id", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyNestedFieldsIfValueIsNotSet() throws IOException {
        TestNestedMessageBQ emptyMessage = TestNestedMessageBQ.newBuilder().build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestNestedMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(protoParser.parse(emptyMessage.toByteArray()), configuration, jsonPrinter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoOdpfParsedMessage.getFieldByName("single_message.order_number", odpfMessageSchema));
        Assert.assertEquals("Invalid field config : single_message.order_number", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfColumnIsNotNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestNestedMessageBQ", descriptorsMap);
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(protoParser.parse(nestedMessage.toByteArray()), configuration, jsonPrinter);
        Assert.assertEquals("test", protoOdpfParsedMessage.getFieldByName("nested_id", odpfMessageSchema));
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoOdpfParsedMessage.getFieldByName("nested_id.order_id", odpfMessageSchema));
        Assert.assertEquals("Invalid field config : nested_id.order_id", exception.getMessage());
    }


    @Test
    public void shouldThrowExceptionIfFieldIsEmpty() throws IOException {
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(dynamicMessage, configuration, jsonPrinter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoOdpfParsedMessage.getFieldByName("", odpfMessageSchema));
        Assert.assertEquals("Invalid field config : name can not be empty", exception.getMessage());
    }

    @Test
    public void shouldThrowDeSerializationExceptionIfJSONPrintFails() throws IOException {
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        Timestamp failingDate = Timestamp.newBuilder().setSeconds(2147483647).setNanos(1000000000).build();
        TestMessageBQ message = TestMessageBQ.newBuilder().setCreatedAt(failingDate).build();
        DynamicMessage message1 = parser.parse(message.toByteArray());
        ProtoOdpfParsedMessage protoOdpfParsedMessage = new ProtoOdpfParsedMessage(message1, configuration, jsonPrinter);
        DeserializerException exception = assertThrows(DeserializerException.class, () -> protoOdpfParsedMessage.getFieldByName("created_at", odpfMessageSchema));
        Assert.assertEquals("Unable to convert proto to JSON: Timestamp is not valid. See proto definition for valid values. Seconds (2147483647) must be in range [-62,135,596,800, +253,402,300,799]. Nanos (1000000000) must be in range [0, +999,999,999].", exception.getMessage());
    }
}
