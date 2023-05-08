package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.*;
import com.gotocompany.depot.*;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.message.proto.TestProtoUtil;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Mockito.CALLS_REAL_METHODS;


public class BigQueryProtoStorageClientTest {


    private Descriptors.Descriptor testDescriptor;
    private BigQueryProtoStorageClient converter;
    private TableSchema testMessageBQSchema;
    private ProtoMessageParser protoMessageParser;

    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessageBQ");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
        ClassLoadStencilClient stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        protoMessageParser = new ProtoMessageParser(stencilClient);
        testMessageBQSchema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_offset")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_topic")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("load_time")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.TIMESTAMP)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_timestamp")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.TIMESTAMP)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_partition")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("order_number")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("created_at")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.DATETIME)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("aliases")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("discount")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("order_url")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("price")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.DOUBLE)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("user_token")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.BYTES)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("counter")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("status")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("properties")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("trip_duration")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("seconds")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("nanos")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("current_state")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("key")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("value")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("updated_at")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.DATETIME)
                        .build())
                .build();
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(testMessageBQSchema);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoStorageClient(writer, config, protoMessageParser);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);
    }

    @Test
    public void shouldConvertPrimitiveFields() throws Exception {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setOrderNumber("order-no-112")
                .setOrderUrl("order-url-1")
                .setDiscount(1200L)
                .setPrice(23)
                .setUserToken(ByteString.copyFrom("test-token".getBytes()))
                .setCounter(20)
                .setStatus(StatusBQ.COMPLETED)
                .addAliases("alias1").addAliases("alias2")
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        Assert.assertEquals(1, payload.getPayloadIndexes().size());
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        Assert.assertEquals("order-no-112", convertedMessage.getField(testDescriptor.findFieldByName("order_number")));
        Assert.assertEquals("order-url-1", convertedMessage.getField(testDescriptor.findFieldByName("order_url")));
        Assert.assertEquals(1200L, convertedMessage.getField(testDescriptor.findFieldByName("discount")));
        Assert.assertEquals(ByteString.copyFrom("test-token".getBytes()), convertedMessage.getField(testDescriptor.findFieldByName("user_token")));
        List aliases = (List) convertedMessage.getField(testDescriptor.findFieldByName("aliases"));
        Assert.assertEquals("alias1", aliases.get(0));
        Assert.assertEquals("alias2", aliases.get(1));
        Assert.assertEquals(20L, convertedMessage.getField(testDescriptor.findFieldByName("counter")));
        Assert.assertEquals("COMPLETED", convertedMessage.getField(testDescriptor.findFieldByName("status")));
    }

    @Test
    public void shouldReturnDurationField() throws IOException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setTripDuration(Duration.newBuilder().setSeconds(1234L).setNanos(231).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        DynamicMessage tripDuration = ((DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("trip_duration")));
        Assert.assertEquals(1234L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("seconds")));
        Assert.assertEquals(231L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("nanos")));
    }

    @Test
    public void shouldReturnMapField() throws Exception {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .putCurrentState("k4", "v4")
                .putCurrentState("k3", "v3")
                .putCurrentState("k1", "v1")
                .putCurrentState("k2", "v2")
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        List<Object> currentState = ((List<Object>) convertedMessage.getField(testDescriptor.findFieldByName("current_state")));
        List<Tuple<String, String>> actual = currentState.stream().map(o -> {
            Map<String, String> values = ((DynamicMessage) o).getAllFields().entrySet().stream().collect(
                    Collectors.toMap(s -> s.getKey().getName(), s -> s.getValue().toString())
            );
            return new Tuple<>(values.get("key"), values.get("value"));
        }).collect(Collectors.toList());
        actual.sort(Comparator.comparing(Tuple::getFirst));
        List<Tuple<String, String>> expected = new ArrayList<Tuple<String, String>>() {{
            add(new Tuple<>("k1", "v1"));
            add(new Tuple<>("k2", "v2"));
            add(new Tuple<>("k3", "v3"));
            add(new Tuple<>("k4", "v4"));
        }};
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldReturnComplexAndNestedType() throws Descriptors.DescriptorValidationException, IOException {
        TableSchema schema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("single_message")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addAllFields(testMessageBQSchema.getFieldsList())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("repeated_message")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addAllFields(testMessageBQSchema.getFieldsList())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("number_field")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("repeated_number_field")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .build();
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestNestedRepeatedMessageBQ");
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoStorageClient(writer, config, protoMessageParser);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);

        Instant now = Instant.now();
        TestMessageBQ singleMessage = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ nested1 = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ nested2 = TestProtoUtil.generateTestMessage(now);
        TestNestedRepeatedMessageBQ message = TestNestedRepeatedMessageBQ.newBuilder()
                .setNumberField(123)
                .setSingleMessage(singleMessage)
                .addRepeatedMessage(nested1)
                .addRepeatedMessage(nested2)
                .addRepeatedNumberField(11)
                .addRepeatedNumberField(12)
                .addRepeatedNumberField(13)
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, message.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);

        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        DynamicMessage sm1 = (DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("single_message"));
        Assert.assertEquals(singleMessage.getOrderNumber(), sm1.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        List<DynamicMessage> nestedMessage = (List) convertedMessage.getField(testDescriptor.findFieldByName("repeated_message"));
        Assert.assertEquals(2, nestedMessage.size());
        DynamicMessage nestedMessage1 = nestedMessage.get(0);
        DynamicMessage nestedMessage2 = nestedMessage.get(1);
        Assert.assertEquals(nested1.getOrderNumber(), nestedMessage1.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals(nested2.getOrderNumber(), nestedMessage2.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals(123L, convertedMessage.getField(testDescriptor.findFieldByName("number_field")));
        List<Long> repeatedNumbers = (List) convertedMessage.getField(testDescriptor.findFieldByName("repeated_number_field"));
        Assert.assertEquals(3, repeatedNumbers.size());
        Assert.assertEquals(Long.valueOf(11), repeatedNumbers.get(0));
        Assert.assertEquals(Long.valueOf(12), repeatedNumbers.get(1));
        Assert.assertEquals(Long.valueOf(13), repeatedNumbers.get(2));
    }

    @Test
    public void shouldConvertTimeStamp() throws IOException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1680609402L).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(1680609402L).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(1680609402L).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        long createdAt = (long) convertedMessage.getField(testDescriptor.findFieldByName("created_at"));
        // Microseconds
        Assert.assertEquals(1680609402000000L, createdAt);
        List<Object> updatedAt = (List) convertedMessage.getField(testDescriptor.findFieldByName("updated_at"));
        Assert.assertEquals(1680609402000000L, updatedAt.get(0));
        Assert.assertEquals(1680609402000000L, updatedAt.get(1));
    }

    @Test
    public void shouldConvertStruct() throws IOException {
        ListValue.Builder builder = ListValue.newBuilder();
        ListValue listValue = builder
                .addValues(Value.newBuilder().setNumberValue(1).build())
                .addValues(Value.newBuilder().setNumberValue(2).build())
                .addValues(Value.newBuilder().setNumberValue(3).build())
                .build();
        Struct value = Struct.newBuilder()
                .putFields("string", Value.newBuilder().setStringValue("string_val").build())
                .putFields("list", Value.newBuilder().setListValue(listValue).build())
                .putFields("boolean", Value.newBuilder().setBoolValue(true).build())
                .putFields("number", Value.newBuilder().setNumberValue(123.45).build())
                .build();

        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setProperties(value)
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        String properties = (String) (convertedMessage.getField(testDescriptor.findFieldByName("properties")));
        String expected = "{\n"
                + "  \"number\": 123.45,\n"
                + "  \"string\": \"string_val\",\n"
                + "  \"list\": [\n"
                + "    1,\n"
                + "    2,\n"
                + "    3\n"
                + "  ],\n"
                + "  \"boolean\": true\n"
                + "}\n";
        JSONAssert.assertEquals(expected, properties, true);
    }

    @Test
    public void shouldHaveMetadataOnPayload() throws InvalidProtocolBufferException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1680609402L).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(
                    null,
                    m1.toByteArray(),
                    new Tuple<>("message_partition", 10),
                    new Tuple<>("message_topic", "test-topic"),
                    new Tuple<>("message_offset", 143),
                    new Tuple<>("load_time", 1680609402L),
                    new Tuple<>("message_timestamp", 1680609402L))
            );
            add(new Message(
                    null,
                    m1.toByteArray(),
                    new Tuple<>("message_partition", 10),
                    new Tuple<>("message_topic", "test-topic"),
                    new Tuple<>("message_offset", 144L),
                    new Tuple<>("load_time", 1680770429L),
                    new Tuple<>("message_timestamp", 1680770429L))
            );
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(2, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        Assert.assertEquals(10L, convertedMessage.getField(testDescriptor.findFieldByName("message_partition")));
        Assert.assertEquals("test-topic", convertedMessage.getField(testDescriptor.findFieldByName("message_topic")));
        Assert.assertEquals(143L, convertedMessage.getField(testDescriptor.findFieldByName("message_offset")));
        Assert.assertEquals(1680609402000L, convertedMessage.getField(testDescriptor.findFieldByName("load_time")));
        Assert.assertEquals(1680609402000L, convertedMessage.getField(testDescriptor.findFieldByName("message_timestamp")));

        serializedRows = protoPayload.getSerializedRows(1);
        convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        Assert.assertEquals(10L, convertedMessage.getField(testDescriptor.findFieldByName("message_partition")));
        Assert.assertEquals("test-topic", convertedMessage.getField(testDescriptor.findFieldByName("message_topic")));
        Assert.assertEquals(144L, convertedMessage.getField(testDescriptor.findFieldByName("message_offset")));
        Assert.assertEquals(1680770429000L, convertedMessage.getField(testDescriptor.findFieldByName("load_time")));
        Assert.assertEquals(1680770429000L, convertedMessage.getField(testDescriptor.findFieldByName("message_timestamp")));
    }


    @Test
    public void shouldHaveMetadataOnPayloadWithNameSpace() throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "__kafka_metadata");
        TableSchema schema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("created_at")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.DATETIME)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("__kafka_metadata")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_offset")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_topic")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("load_time")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.TIMESTAMP)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_timestamp")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.TIMESTAMP)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_partition")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .build())
                .build();
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoStorageClient(writer, config, protoMessageParser);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);

        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1680609402L).build())
                .build();

        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(
                    null,
                    m1.toByteArray(),
                    new Tuple<>("message_partition", 10),
                    new Tuple<>("message_topic", "test-topic"),
                    new Tuple<>("message_offset", 143),
                    new Tuple<>("load_time", 1680609402L),
                    new Tuple<>("message_timestamp", 1680609402L))
            );
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        DynamicMessage metadata = (DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("__kafka_metadata"));
        Assert.assertEquals(10L, metadata.getField(metadata.getDescriptorForType().findFieldByName("message_partition")));
        Assert.assertEquals("test-topic", metadata.getField(metadata.getDescriptorForType().findFieldByName("message_topic")));
        Assert.assertEquals(143L, metadata.getField(metadata.getDescriptorForType().findFieldByName("message_offset")));
        Assert.assertEquals(1680609402000L, metadata.getField(metadata.getDescriptorForType().findFieldByName("load_time")));
        Assert.assertEquals(1680609402000L, metadata.getField(metadata.getDescriptorForType().findFieldByName("message_timestamp")));
    }

    @Test
    public void shouldReturnInvalidRecords() throws InvalidProtocolBufferException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1680609402L).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray(), new Tuple<>("message_offset", 11)));
            add(new Message(null, "invalid".getBytes(StandardCharsets.UTF_8), new Tuple<>("message_offset", 12)));
            add(new Message(null, m1.toByteArray(), new Tuple<>("message_offset", 13)));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(2, protoPayload.getSerializedRowsCount());
        Assert.assertEquals(2, payload.getPayloadIndexes().size());
        Assert.assertTrue(payload.getPayloadIndexes().contains(0L));
        Assert.assertTrue(payload.getPayloadIndexes().contains(1L));
        Assert.assertFalse(payload.getPayloadIndexes().contains(2L));
        Assert.assertEquals(0L, payload.getInputIndex(0L));
        Assert.assertEquals(2L, payload.getInputIndex(1L));
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        long createdAt = (long) convertedMessage.getField(testDescriptor.findFieldByName("created_at"));
        // Microseconds
        Assert.assertEquals(1680609402000000L, createdAt);

        List<BigQueryRecordMeta> metas = new ArrayList<>();
        for (BigQueryRecordMeta r : payload) {
            metas.add(r);
        }
        BigQueryRecordMeta validRecord = metas.get(0);
        BigQueryRecordMeta invalidRecord = metas.get(1);
        Assert.assertTrue(validRecord.isValid());
        Assert.assertFalse(invalidRecord.isValid());
        Assert.assertNull(validRecord.getErrorInfo());
        Assert.assertNotNull(invalidRecord.getErrorInfo());
        Assert.assertEquals(0, validRecord.getInputIndex());
        Assert.assertEquals(1, invalidRecord.getInputIndex());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, invalidRecord.getErrorInfo().getErrorType());
        Assert.assertEquals("While parsing a protocol message, the input ended unexpectedly in the middle of a field.  This could mean either that the input has been truncated or that an embedded message misreported its own length.",
                invalidRecord.getErrorInfo().getException().getMessage());
    }
}
