package io.odpf.sink.connectors.bigquery.converter;

import com.google.api.client.util.DateTime;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.odpf.sink.connectors.TestMessageBQ;
import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.bigquery.models.Records;
import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import io.odpf.sink.connectors.config.Tuple;
import io.odpf.sink.connectors.error.ErrorType;
import io.odpf.sink.connectors.message.*;
import io.odpf.sink.connectors.bigquery.TestOdpfMessageBuilder;
import io.odpf.sink.connectors.bigquery.TestMetadata;
import io.odpf.sink.connectors.message.proto.ProtoOdpfMessage;
import io.odpf.sink.connectors.message.proto.ProtoOdpfMessageParser;
import io.odpf.sink.connectors.message.proto.ProtoOdpfParsedMessage;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
 import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MessageRecordConverterTest {
    private MessageRecordConverter recordConverter;
    private RowMapper rowMapper;
    private StencilClient stencilClient;
    private Instant now;

    @Before
    public void setUp() {
        System.setProperty("INPUT_SCHEMA_PROTO_CLASS", "io.odpf.sink.connectors.TestMessageBQ");
        stencilClient = StencilClientFactory.getClient();
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        Properties columnMapping = new Properties();
        columnMapping.put(1, "bq_order_number");
        columnMapping.put(2, "bq_order_url");
        columnMapping.put(3, "bq_order_details");
        rowMapper = new RowMapper(columnMapping);

        recordConverter = new MessageRecordConverter(rowMapper, protoOdpfMessageParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));

        now = Instant.now();
    }

    @Test
    public void shouldGetRecordForBQFromConsumerRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        OdpfMessage record2 = TestOdpfMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");


        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestOdpfMessageBuilder.metadataColumns(record1Offset, now));


        Map<String, Object> record2ExpectedColumns = new HashMap<>();
        record2ExpectedColumns.put("bq_order_number", "order-2");
        record2ExpectedColumns.put("bq_order_url", "order-url-2");
        record2ExpectedColumns.put("bq_order_details", "order-details-2");
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
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
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
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
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
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(rowMapper, protoOdpfMessageParser, sinkConfig);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
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
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(rowMapper, protoOdpfMessageParser, sinkConfig);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        OdpfMessage record1 = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
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
        OdpfMessage record2 = new ProtoOdpfMessage("invalid-key".getBytes(), "invalid-value".getBytes(),
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

        OdpfMessage record2 = new ProtoOdpfMessage("invalid-key".getBytes(), "invalid-value".getBytes(),
                new Tuple<>("message_topic", record2Offset.getTopic()),
                new Tuple<>("message_partition", record2Offset.getPartition()),
                new Tuple<>("message_offset", record2Offset.getOffset()),
                new Tuple<>("message_timestamp", new DateTime(record2Offset.getTimestamp())),
                new Tuple<>("load_time", new DateTime(record2Offset.getLoadTime())));

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
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
        System.setProperty("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "false");
        OdpfMessageParser mockParser = mock(OdpfMessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        OdpfMessage consumerRecord = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessageBQ.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        ParsedOdpfMessage parsedOdpfMessage = new ProtoOdpfParsedMessage(dynamicMessage);
        when(mockParser.parse(consumerRecord, InputSchemaMessageMode.LOG_MESSAGE, "io.odpf.sink.connectors.TestMessageBQ")).thenReturn(parsedOdpfMessage);

        recordConverter = new MessageRecordConverter(rowMapper, mockParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));

        List<OdpfMessage> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages);

        assertEquals(0, records.getValidRecords().size());
        assertEquals(1, records.getInvalidRecords().size());
        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
        assertEquals(consumerRecord.getMetadata(), records.getInvalidRecords().get(0).getMetadata());
    }

    @Test
    public void shouldIgnoreUnknownFieldsIfTheConfigIsSet() throws IOException {
        System.setProperty("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "true");
        OdpfMessageParser mockParser = mock(OdpfMessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        OdpfMessage consumerRecord = TestOdpfMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessageBQ.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        ParsedOdpfMessage parsedOdpfMessage = new ProtoOdpfParsedMessage(dynamicMessage);
        when(mockParser.parse(consumerRecord, InputSchemaMessageMode.LOG_MESSAGE, "io.odpf.sink.connectors.TestMessageBQ")).thenReturn(parsedOdpfMessage);

        recordConverter = new MessageRecordConverter(rowMapper, mockParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));

        List<OdpfMessage> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages);
        Record record = new Record(consumerRecord.getMetadata(), consumerRecord.getMetadata(), 0, null);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        assertEquals(record, records.getValidRecords().get(0));
    }
}
