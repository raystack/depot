package com.gotocompany.depot.bigquery.converter;

import com.google.api.client.util.DateTime;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import com.gotocompany.depot.StatusBQ;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.bigquery.TestMessageBuilder;
import com.gotocompany.depot.bigquery.TestMetadata;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.gotocompany.stencil.client.StencilClient;
import com.jayway.jsonpath.Configuration;
import groovy.lang.Tuple3;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageRecordConverterTest {
    private MessageRecordConverter recordConverter;
    @Mock
    private ClassLoadStencilClient stencilClient;
    private Instant now;

    @Before
    public void setUp() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);

        recordConverter = new MessageRecordConverter(protoMessageParser, bigQuerySinkConfig);

        now = Instant.now();
    }

    @Test
    public void shouldGetRecordForBQFromConsumerRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");


        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));


        Map<String, Object> record2ExpectedColumns = new HashMap<>();
        record2ExpectedColumns.put("order_number", "order-2");
        record2ExpectedColumns.put("order_url", "order-url-2");
        record2ExpectedColumns.put("order_details", "order-details-2");
        record2ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record2Offset, now));
        List<Message> messages = Arrays.asList(record1, record2);

        Records records = recordConverter.convert(messages);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        Map<String, Object> record2Columns = records.getValidRecords().get(1).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record2ExpectedColumns.size(), record2Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(record2ExpectedColumns, record2Columns);
    }

    @Test
    public void shouldIgnoreNullRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = TestMessageBuilder.withMetadata(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");


        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test
    public void shouldReturnInvalidRecordsWhenGivenNullRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = TestMessageBuilder.withMetadata(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test
    public void shouldNotNamespaceMetadataFieldWhenNamespaceIsNotProvided() {
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(protoMessageParser, sinkConfig);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Collections.singletonList(record1);
        Records records = recordConverterTest.convert(messages);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(sinkConfig.getBqMetadataNamespace(), "");
    }

    @Test
    public void shouldNamespaceMetadataFieldWhenNamespaceIsProvided() {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "metadata_ns");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(protoMessageParser, sinkConfig);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.put(sinkConfig.getBqMetadataNamespace(), TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Collections.singletonList(record1);
        Records records = recordConverterTest.convert(messages);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
    }


    @Test
    public void shouldReturnInvalidRecordsGivenInvalidProtobufMessage() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");
        Message record2 = new Message("invalid-key".getBytes(), "invalid-value".getBytes(),
                new Tuple<>("topic", record2Offset.getTopic()),
                new Tuple<>("partition", record2Offset.getPartition()));
        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);
        assertEquals(1, records.getInvalidRecords().size());
    }

    @Test
    public void shouldWriteToErrorWriterInvalidRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        Message record2 = new Message("invalid-key".getBytes(), "invalid-value".getBytes(),
                new Tuple<>("message_topic", record2Offset.getTopic()),
                new Tuple<>("message_partition", record2Offset.getPartition()),
                new Tuple<>("message_offset", record2Offset.getOffset()),
                new Tuple<>("message_timestamp", new DateTime(record2Offset.getTimestamp())),
                new Tuple<>("load_time", new DateTime(record2Offset.getLoadTime())));

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        assertEquals(1, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(new HashMap<>(), records.getInvalidRecords().get(0).getColumns());
        assertEquals(record2.getMetadata(), records.getInvalidRecords().get(0).getMetadata());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
    }

    @Test
    public void shouldReturnInvalidRecordsWhenUnknownFieldsFound() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "false");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        MessageParser mockParser = mock(MessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Message consumerRecord = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ParsedMessage parsedMessage = new ProtoParsedMessage(dynamicMessage, jsonPathConfig);

        when(mockParser.parse(consumerRecord, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage")).thenReturn(parsedMessage);

        recordConverter = new MessageRecordConverter(mockParser, bigQuerySinkConfig);

        List<Message> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages);

        assertEquals(0, records.getValidRecords().size());
        assertEquals(1, records.getInvalidRecords().size());
        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
        assertEquals(consumerRecord.getMetadata(), records.getInvalidRecords().get(0).getMetadata());
    }

    @Test
    public void shouldIgnoreUnknownFieldsIfTheConfigIsSet() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "true");
        MessageParser mockParser = mock(MessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Message consumerRecord = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(10, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ParsedMessage parsedMessage = new ProtoParsedMessage(dynamicMessage, jsonPathConfig);
        when(mockParser.parse(consumerRecord, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage")).thenReturn(parsedMessage);

        recordConverter = new MessageRecordConverter(mockParser, bigQuerySinkConfig
        );

        List<Message> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages);

        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
        Map<String, Object> metadata = consumerRecord.getMetadata();
        Map<String, Object> finalMetadata = metadataColumnsTypes.stream().collect(Collectors.toMap(TupleString::getFirst, t -> {
            String key = t.getFirst();
            String dataType = t.getSecond();
            Object value = metadata.get(key);
            if (value instanceof Long && dataType.equals("timestamp")) {
                value = new DateTime((long) value);
            }
            return value;
        }));
        Record record = new Record(consumerRecord.getMetadata(), finalMetadata, 0, null);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        assertEquals(record, records.getValidRecords().get(0));
    }

    private Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> setupForTypeTest(String fieldName, Object value) throws InvalidProtocolBufferException {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        Descriptors.FieldDescriptor fd = TestMessageBQ.getDescriptor().findFieldByName(fieldName);
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setField(fd, value)
                .build();
        DynamicMessage d = DynamicMessage.parseFrom(TestMessageBQ.getDescriptor(), message.toByteArray());
        Message consumerRecord = new Message(
                message.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", record1Offset.getTopic()),
                new Tuple<>("message_partition", record1Offset.getPartition()),
                new Tuple<>("message_offset", record1Offset.getOffset()),
                new Tuple<>("message_timestamp", record1Offset.getTimestamp()),
                new Tuple<>("load_time", record1Offset.getLoadTime()));
        List<Message> messages = Collections.singletonList(consumerRecord);
        StencilClient client1 = Mockito.mock(StencilClient.class);
        when(client1.parse(anyString(), any())).thenReturn(d);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(client1, jsonPathConfig);
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(protoMessageParser,
                bigQuerySinkConfig);
        Map<String, Object> metadataColumns = TestMessageBuilder.metadataColumns(record1Offset, now);
        return new Tuple3<>(messageRecordConverter, messages, metadataColumns);
    }

    @Test
    public void shouldConvertTimestampFieldToDateTime() throws IOException {
        Timestamp timestampData = Timestamps.fromMillis(now.toEpochMilli());
        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("created_at", timestampData);
        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();
        DateTime expectedDayTime = new DateTime(now.toEpochMilli());

        Records records = converter.convert(inputData);

        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(expectedDayTime, record1Columns.get("created_at"));
    }

    @Test
    public void shouldConvertStructFieldToMap() throws IOException {
        Struct structData = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("goto").build())
                .putFields("age", Value.newBuilder().setNumberValue(Double.parseDouble("10")).build())
                .putFields("null_key", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putFields("bool", Value.newBuilder().setBoolValue(true).build())
                .build();
        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("properties", structData);
        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();

        String expectedProperties = "{\"name\":\"goto\",\"age\":10,\"bool\": true, \"null_key\": null}";

        Records records = converter.convert(inputData);

        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();

        assertEquals(new JSONObject(expectedProperties).toString(), new JSONObject((String) record1Columns.get("properties")).toString());
    }

    @Test
    public void shouldThrowExceptionWhenFloatingPointIsNaN() throws IOException {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestTypesMessage testTypesMessage = TestTypesMessage.newBuilder().setFloatValue(Float.NaN).setDoubleValue(Double.NaN).setStringValue("test").build();
        DynamicMessage message = DynamicMessage.parseFrom(TestTypesMessage.getDescriptor(), testTypesMessage.toByteArray());
        Message consumerRecord = new Message(
                message.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", record1Offset.getTopic()),
                new Tuple<>("message_partition", record1Offset.getPartition()),
                new Tuple<>("message_offset", record1Offset.getOffset()),
                new Tuple<>("message_timestamp", record1Offset.getTimestamp()),
                new Tuple<>("load_time", record1Offset.getLoadTime()));
        List<Message> messages = Collections.singletonList(consumerRecord);
        StencilClient client1 = Mockito.mock(StencilClient.class);
        when(client1.parse(anyString(), any())).thenReturn(message);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(client1, jsonPathConfig);
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(protoMessageParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));
        Records records = messageRecordConverter.convert(messages);
        assertEquals(IllegalArgumentException.class, records.getInvalidRecords().get(0).getErrorInfo().getException().getClass());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
    }

    @Test
    public void shouldThrowExceptionWhenDoubleIsNaN() throws IOException {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestTypesMessage testTypesMessage = TestTypesMessage.newBuilder().setDoubleValue(Double.NaN).setStringValue("test").build();
        DynamicMessage message = DynamicMessage.parseFrom(TestTypesMessage.getDescriptor(), testTypesMessage.toByteArray());
        Message consumerRecord = new Message(
                message.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", record1Offset.getTopic()),
                new Tuple<>("message_partition", record1Offset.getPartition()),
                new Tuple<>("message_offset", record1Offset.getOffset()),
                new Tuple<>("message_timestamp", record1Offset.getTimestamp()),
                new Tuple<>("load_time", record1Offset.getLoadTime()));
        List<Message> messages = Collections.singletonList(consumerRecord);
        StencilClient client1 = Mockito.mock(StencilClient.class);
        when(client1.parse(anyString(), any())).thenReturn(message);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(client1, jsonPathConfig);
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(protoMessageParser,
                bigQuerySinkConfig);
        Records records = messageRecordConverter.convert(messages);
        assertEquals(IllegalArgumentException.class, records.getInvalidRecords().get(0).getErrorInfo().getException().getClass());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
    }

    @Test
    public void shouldConvertEnumToString() throws IOException {

        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("status", StatusBQ.CANCELLED.getValueDescriptor());

        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();

        Records records = converter.convert(inputData);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();

        assertEquals("CANCELLED", record1Columns.get("status"));
    }

    @Test
    public void shouldConvertBytesToString() throws IOException {
        byte[] byteData = "byteDataTest".getBytes(StandardCharsets.UTF_8);
        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("user_token", ByteString.copyFrom(byteData));

        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();

        Records records = converter.convert(inputData);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        String expected = BaseEncoding.base64().encode(byteData);

        assertEquals(expected, record1Columns.get("user_token"));
    }
}
