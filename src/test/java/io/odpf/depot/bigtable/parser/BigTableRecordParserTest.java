package io.odpf.depot.bigtable.parser;

import com.google.protobuf.Timestamp;
import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestLocation;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.model.BigTableSchema;
import io.odpf.depot.common.Template;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.utils.MessageConfigUtils;
import io.odpf.stencil.client.ClassLoadStencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class BigTableRecordParserTest {

    @Mock
    private ClassLoadStencilClient stencilClient;
    @Mock
    private OdpfMessageSchema schema;
    @Mock
    private OdpfMessageParser mockOdpfMessageParser;
    @Mock
    private BigTableRowKeyParser mockBigTableRowKeyParser;
    @Mock
    private ParsedOdpfMessage mockParsedOdpfMessage;
    private BigTableRecordParser bigTableRecordParser;
    private List<OdpfMessage> messages;
    private BigTableSinkConfig sinkConfig;

    @Before
    public void setUp() throws IOException, InvalidTemplateException {
        MockitoAnnotations.openMocks(this);
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\"} }");
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key-constant-string");


        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100L).setNanos(200).build())
                .setServiceType(TestServiceType.Enum.GO_SEND)
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(100D).setLongitude(200D).build())
                .build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(101L).setNanos(202).build())
                .setServiceType(TestServiceType.Enum.GO_SHOP)
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(300D).setLongitude(400D).build())
                .build();

        OdpfMessage message1 = new OdpfMessage(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        OdpfMessage message2 = new OdpfMessage(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());
        messages = Collections.list(message1, message2);

        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);

        bigTableRecordParser = new BigTableRecordParser(protoOdpfMessageParser, bigTableRowKeyParser, modeAndSchema, schema, bigtableSchema);
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidOdpfMessages() {
        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidOdpfMessagesForComplexFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location\"} }");
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoOdpfMessageParser, bigTableRowKeyParser, modeAndSchema, schema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidOdpfMessagesForNestedTimestampFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"event_timestamp.nanos\"} }");
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoOdpfMessageParser, bigTableRowKeyParser, modeAndSchema, schema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidOdpfMessagesForNestedFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location.latitude\"} }");
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoOdpfMessageParser, bigTableRowKeyParser, modeAndSchema, schema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnInvalidRecordForAnyNullOdpfMessage() {
        List<BigTableRecord> records = bigTableRecordParser.convert(Collections.list(new OdpfMessage(null, null)));
        assertFalse(records.get(0).isValid());
        assertNotNull(records.get(0).getErrorInfo());
    }

    @Test
    public void shouldCatchEmptyMessageExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsInvalidMessageError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockOdpfMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                schema,
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockOdpfMessageParser.parse(any(), any(), any())).thenThrow(EmptyMessageException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.INVALID_MESSAGE_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    @Test
    public void shouldCatchConfigurationExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockOdpfMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                schema,
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockOdpfMessageParser.parse(any(), any(), any())).thenThrow(ConfigurationException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    @Test
    public void shouldCatchIOExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsDeserializationError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockOdpfMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                schema,
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockOdpfMessageParser.parse(any(), any(), any())).thenThrow(IOException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.DESERIALIZATION_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    @Test
    public void shouldCatchIllegalArgumentExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockOdpfMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                schema,
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockOdpfMessageParser.parse(any(), any(), any())).thenReturn(mockParsedOdpfMessage);
        when(mockBigTableRowKeyParser.parse(mockParsedOdpfMessage)).thenThrow(IllegalArgumentException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
        }
    }
}
