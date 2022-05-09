package io.odpf.depot.bigquery.handler;

import com.google.api.client.util.DateTime;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.odpf.depot.TestMessage;
import io.odpf.depot.common.TupleString;
import io.odpf.depot.message.*;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.message.proto.ProtoOdpfParsedMessage;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.bigquery.models.Records;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.bigquery.TestOdpfMessageBuilder;
import io.odpf.depot.bigquery.TestMetadata;
import io.odpf.stencil.client.ClassLoadStencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MessageRecordConverterTest {
    private MessageRecordConverter recordConverter;
    @Mock
    private ClassLoadStencilClient stencilClient;
    private Instant now;
    private OdpfMessageSchema schema;

    @Before
    public void setUp() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_CLASS", "io.odpf.depot.TestMessage");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
        }};
        doReturn(descriptorsMap).when(stencilClient).getAll();
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        schema = protoOdpfMessageParser.getSchema("io.odpf.depot.TestMessage");
        recordConverter = new MessageRecordConverter(protoOdpfMessageParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()), schema);

        now = Instant.now();
    }

    @Test
    public void shouldGetRecordForBQFromConsumerRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        OdpfMessage record2 = TestOdpfMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");


        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestOdpfMessageBuilder.metadataColumns(record1Offset, now));


        Map<String, Object> record2ExpectedColumns = new HashMap<>();
        record2ExpectedColumns.put("order_number", "order-2");
        record2ExpectedColumns.put("order_url", "order-url-2");
        record2ExpectedColumns.put("order_details", "order-details-2");
        record2ExpectedColumns.putAll(TestOdpfMessageBuilder.metadataColumns(record2Offset, now));
        List<OdpfMessage> messages = Arrays.asList(record1, record2);

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
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        OdpfMessage record2 = TestOdpfMessageBuilder.withMetadata(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");


        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestOdpfMessageBuilder.metadataColumns(record1Offset, now));

        List<OdpfMessage> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    public void shouldReturnInvalidRecordsWhenGivenNullRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        OdpfMessage record2 = TestOdpfMessageBuilder.withMetadata(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestOdpfMessageBuilder.metadataColumns(record1Offset, now));

        List<OdpfMessage> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test
    public void shouldNotNamespaceMetadataFieldWhenNamespaceIsNotProvided() {
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(protoOdpfMessageParser, sinkConfig, schema);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestOdpfMessageBuilder.metadataColumns(record1Offset, now));

        List<OdpfMessage> messages = Collections.singletonList(record1);
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
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(protoOdpfMessageParser, sinkConfig, schema);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.put(sinkConfig.getBqMetadataNamespace(), TestOdpfMessageBuilder.metadataColumns(record1Offset, now));

        List<OdpfMessage> messages = Collections.singletonList(record1);
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
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");
        OdpfMessage record2 = new OdpfMessage("invalid-key".getBytes(), "invalid-value".getBytes(),
                new Tuple<>("topic", record2Offset.getTopic()),
                new Tuple<>("partition", record2Offset.getPartition()));
        List<OdpfMessage> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);
        assertEquals(1, records.getInvalidRecords().size());
    }

    @Test
    public void shouldWriteToErrorWriterInvalidRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        OdpfMessage record2 = new OdpfMessage("invalid-key".getBytes(), "invalid-value".getBytes(),
                new Tuple<>("message_topic", record2Offset.getTopic()),
                new Tuple<>("message_partition", record2Offset.getPartition()),
                new Tuple<>("message_offset", record2Offset.getOffset()),
                new Tuple<>("message_timestamp", new DateTime(record2Offset.getTimestamp())),
                new Tuple<>("load_time", new DateTime(record2Offset.getLoadTime())));

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestOdpfMessageBuilder.metadataColumns(record1Offset, now));

        List<OdpfMessage> messages = Arrays.asList(record1, record2);
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
        OdpfMessageParser mockParser = mock(OdpfMessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        OdpfMessage consumerRecord = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        ParsedOdpfMessage parsedOdpfMessage = new ProtoOdpfParsedMessage(dynamicMessage);
        when(mockParser.parse(consumerRecord, InputSchemaMessageMode.LOG_MESSAGE, "io.odpf.depot.TestMessage")).thenReturn(parsedOdpfMessage);

        recordConverter = new MessageRecordConverter(mockParser, ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()), schema);

        List<OdpfMessage> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages);

        assertEquals(0, records.getValidRecords().size());
        assertEquals(1, records.getInvalidRecords().size());
        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
        assertEquals(consumerRecord.getMetadata(), records.getInvalidRecords().get(0).getMetadata());
    }

    @Test
    public void shouldIgnoreUnknownFieldsIfTheConfigIsSet() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "true");
        OdpfMessageParser mockParser = mock(OdpfMessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        OdpfMessage consumerRecord = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        ParsedOdpfMessage parsedOdpfMessage = new ProtoOdpfParsedMessage(dynamicMessage);
        when(mockParser.parse(consumerRecord, InputSchemaMessageMode.LOG_MESSAGE, "io.odpf.depot.TestMessage")).thenReturn(parsedOdpfMessage);

        recordConverter = new MessageRecordConverter(mockParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()), schema);

        List<OdpfMessage> messages = Collections.singletonList(consumerRecord);
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
}
