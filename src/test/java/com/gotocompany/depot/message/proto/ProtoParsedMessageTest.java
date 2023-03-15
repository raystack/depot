package com.gotocompany.depot.message.proto;

import com.google.api.client.util.DateTime;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.FloatTest;
import com.gotocompany.depot.FloatTestContainer;
import com.gotocompany.depot.StatusBQ;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestKeyBQ;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.TestNestedMessageBQ;
import com.gotocompany.depot.TestNestedRepeatedMessageBQ;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.stencil.Parser;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.xerces.impl.dv.util.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
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
import static org.junit.Assert.assertThrows;

public class ProtoParsedMessageTest {

    private static final JsonFormat.Printer PRINTER = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();
    private Timestamp createdAt;
    private DynamicMessage dynamicMessage;
    private Instant now;
    private long nowMillis;
    private ProtoMessageParser messageParser;
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
            put(String.format("%s", TestTypesMessage.class.getName()), TestTypesMessage.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", FloatTest.class.getName()), FloatTest.getDescriptor());
            put(String.format("%s", FloatTestContainer.class.getName()), FloatTestContainer.getDescriptor());
            put("com.gotocompany.depot.TestMessageBQ.CurrentStateEntry", TestMessageBQ.getDescriptor().getNestedTypes().get(0));
            put("com.google.protobuf.Struct.FieldsEntry", Struct.getDescriptor().getNestedTypes().get(0));
            put("com.google.protobuf.Duration", com.google.protobuf.Duration.getDescriptor());
            put("com.google.type.Date", com.google.type.Date.getDescriptor());
            put("google.protobuf.BoolValue", com.google.protobuf.BoolValue.getDescriptor());
        }};
        messageParser = new ProtoMessageParser(stencilClient);
    }

    @Test
    public void shouldReturnFieldsInProperties() throws IOException {
        Map<String, Object> fields = new ProtoParsedMessage(dynamicMessage).getMapping();
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
    public void shouldThrowExceptionWhenFloatingPointIsNaN() throws IOException {
        String data = "ogQFJQAAwH8=";
        byte[] decode = Base64.decode(data);
        DynamicMessage message = DynamicMessage.parseFrom(FloatTest.getDescriptor(), decode);
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ProtoParsedMessage(message).getMapping());
    }

    @Test
    public void shouldParseDurationMessageSuccessfully() throws IOException {
        TestMessageBQ message = TestProtoUtil.generateTestMessage(now);
        Parser messageProtoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        Map<String, Object> fields = new ProtoParsedMessage(messageProtoParser.parse(message.toByteArray())).getMapping();
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
                fields = new ProtoParsedMessage(protoParser.parse(msg.toByteArray()))
                        .getMapping();
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
        Map<String, Object> fields = new ProtoParsedMessage(protoParser.parse(message.toByteArray())).getMapping();

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
        Map<String, Object> fields = new ProtoParsedMessage(protoParser.parse(message.toByteArray())).getMapping();

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
        Map<String, Object> fields = new ProtoParsedMessage(protoParser.parse(message.toByteArray())).getMapping();

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
        Map<String, Object> fields = new ProtoParsedMessage(protoParser.parse(message.toByteArray())).getMapping();

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
        Map<String, Object> fields = new ProtoParsedMessage(protoParser.parse(message.toByteArray())).getMapping();


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
        Map<String, Object> fields = new ProtoParsedMessage(dynamicMessage).getMapping();

        assertNull(fields.get("single_message"));
    }

    @Test
    public void shouldReturnNullWhenNoDateFieldIsProvided() throws IOException {
        TestMessageBQ testMessage = TestMessageBQ.newBuilder()
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        dynamicMessage = protoParser.parse(testMessage.toByteArray());
        Map<String, Object> fields = new ProtoParsedMessage(dynamicMessage).getMapping();

        assertNull(fields.get("order_date"));
    }

    @Test
    public void shouldParseRepeatedTimestamp() throws IOException {
        createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();

        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addUpdatedAt(createdAt).addUpdatedAt(createdAt)
                .setCreatedAt(createdAt)
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());

        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<String, Object> fields = protoParsedMessage.getMapping();

        assertEquals(Arrays.asList(new DateTime(now.toEpochMilli()), new DateTime(now.toEpochMilli())), fields.get("updated_at"));
    }

    @Test
    public void shouldParseStructField() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setProperties(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setStringValue("50").build()).build())
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());

        Map<String, Object> fields = new ProtoParsedMessage(protoParser.parse(message.toByteArray())).getMapping();

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
        Map<String, Object> fields = new ProtoParsedMessage(protoParser.parse(message.toByteArray())).getMapping();
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
        ParsedMessage parsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<String, Object> map1 = parsedMessage.getMapping();
        Map<String, Object> map2 = parsedMessage.getMapping();
        assertEquals(map1, map2);
    }


    @Test
    public void shouldGetFieldByName() throws IOException {
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(dynamicMessage);
        Object orderNumber = protoParsedMessage.getFieldByName("order_number");
        Assert.assertEquals("order-1", orderNumber);
    }

    @Test
    public void shouldGetComplexFieldByName() throws IOException {
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(123)
                        .setTopic("my-topic").build())
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        DynamicMessage bookingLogDynamicMessage = protoParser.parse(testBookingLogMessage.toByteArray());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(bookingLogDynamicMessage);
        JSONArray f = (JSONArray) protoParsedMessage.getFieldByName("topics");

        Assert.assertEquals(new JSONObject("{\"qos\": 1, \"topic\": \"hellowo/rl/dcom.world.partner\"}").toString(),
                f.get(0).toString());
        Assert.assertEquals(new JSONObject("{\"qos\": 123, \"topic\": \"my-topic\"}").toString(),
                f.get(1).toString());
    }


    @Test
    public void shouldGetStructFromProto() throws IOException {
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(10.2).setLongitude(12.01).build())
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        DynamicMessage bookingLogDynamicMessage = protoParser.parse(testBookingLogMessage.toByteArray());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(bookingLogDynamicMessage);
        Object driverPickupLocation = protoParsedMessage.getFieldByName("driver_pickup_location");
        Assert.assertEquals(PRINTER.print(TestLocation.newBuilder().setLatitude(10.2).setLongitude(12.01).build()), driverPickupLocation.toString());
    }

    @Test
    public void shouldGetRepeatableStructField() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50.02).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(60.1).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("active", Value.newBuilder().setBoolValue(true).build())
                        .putFields("height", Value.newBuilder().setNumberValue(175.9).build()).build())
                .build();

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        JSONArray attributes = (JSONArray) protoParsedMessage.getFieldByName("attributes");
        protoParsedMessage.getMapping();
        for (int ii = 0; ii < message.getAttributesCount(); ii++) {
            Assert.assertEquals(PRINTER.print(message.getAttributes(ii)), attributes.get(ii).toString());
        }
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
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Object discount = protoParsedMessage.getFieldByName("discount");
        Assert.assertEquals(10000012010L, discount);
        Object price = protoParsedMessage.getFieldByName("price");
        Assert.assertEquals(Float.valueOf(10.2f).toString(), price.toString());
    }

    @Test
    public void shouldGetRepeatedTimeStamps() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message1.toByteArray()));
        JSONArray updatedTimeStamps = (JSONArray) protoParsedMessage.getFieldByName("updated_at");
        Assert.assertEquals(2, updatedTimeStamps.length());
        Assert.assertEquals(now.toString(), updatedTimeStamps.get(0));
        Assert.assertEquals(now.toString(), updatedTimeStamps.get(1));
    }


    @Test
    public void shouldGetFieldByNameFromNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()));
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        Object orderNumber = protoParsedMessage.getFieldByName("single_message.order_number");
        Assert.assertEquals(message1.getOrderNumber(), orderNumber);
    }

    @Test
    public void shouldReturnInstantField() throws IOException {
        Instant time = Instant.ofEpochSecond(1669160207, 600000000);
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(time);
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()));
        Assert.assertEquals(time.toString(), protoParsedMessage.getFieldByName("created_at"));
    }

    @Test
    public void shouldReturnDurationFieldInStringFormat() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()));
        Object tripDuration = protoParsedMessage.getFieldByName("trip_duration");
        // ProtoFormat Printer returns valid json Value (String with double quotes).
        // Whereas JSONObject, JSONArray returns java types.
        // To return valid value we have to use JSONWriter.
        Assert.assertEquals(
                PRINTER.print(Duration.newBuilder().setSeconds(1).setNanos(TestProtoUtil.TRIP_DURATION_NANOS).build()),
                JSONWriter.valueToString(tripDuration.toString()));
    }

    @Test
    public void shouldReturnMapFieldAsJSONObject() throws IOException {
        TestMessageBQ message1 = TestMessageBQ.newBuilder().putCurrentState("running", "active").build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()));
        Object currentState = protoParsedMessage.getFieldByName("current_state");
        Assert.assertEquals("{\"running\":\"active\"}", currentState.toString());
    }

    @Test
    public void shouldReturnDefaultValueForFieldIfValueIsNotSet() throws IOException {
        TestMessageBQ emptyMessage = TestMessageBQ.newBuilder().build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(emptyMessage.toByteArray()));
        Object orderNumber = protoParsedMessage.getFieldByName("order_number");
        Assert.assertEquals("", orderNumber);
    }

    @Test
    public void shouldThrowExceptionIfColumnIsNotPresentInProto() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()));
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName("single_message.order_id"));
        Assert.assertEquals("Invalid field config : single_message.order_id", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfColumnIsNotNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()));
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName("nested_id.order_id"));
        Assert.assertEquals("Invalid field config : nested_id.order_id", exception.getMessage());
    }


    @Test
    public void shouldThrowExceptionIfFieldIsEmpty() throws IOException {
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(dynamicMessage);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName(""));
        Assert.assertEquals("Invalid field config : name can not be empty", exception.getMessage());
    }

    @Test
    public void shouldReturnRepeatedDurations() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message1.toByteArray()));
        JSONArray intervals = (JSONArray) protoParsedMessage.getFieldByName("intervals");
        Assert.assertEquals(PRINTER.print(Duration.newBuilder().setSeconds(12).setNanos(1000).build()), JSONWriter.valueToString(intervals.get(0)));
        Assert.assertEquals(PRINTER.print(Duration.newBuilder().setSeconds(15).setNanos(1000).build()), JSONWriter.valueToString(intervals.get(1)));
    }

    @Test
    public void shouldReturnRepeatedString() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListValues("test1")
                .addListValues("test2")
                .addListValues("test3")
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        JSONArray listValues = (JSONArray) protoParsedMessage.getFieldByName("list_values");
        Assert.assertEquals("test1", listValues.get(0));
        Assert.assertEquals("test2", listValues.get(1));
        Assert.assertEquals("test3", listValues.get(2));
    }

    @Test
    public void shouldReturnValueAtSpecificIndexInRepeatedField() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListValues("test1")
                .addListValues("test2")
                .addListValues("test3")
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Object value = protoParsedMessage.getFieldByName("list_values[1]");
        Assert.assertEquals("test2", value.toString());
    }

    @Test
    public void shouldReturnValueAtSpecificIndexInRepeatedMessageField() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListMessageValues(TestMessage.newBuilder().setOrderNumber("123"))
                .addListMessageValues(TestMessage.newBuilder().setOrderNumber("456"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Object value = protoParsedMessage.getFieldByName("list_message_values[0]");
        Assert.assertEquals("{\"order_number\":\"123\"}", value.toString());
    }
}
