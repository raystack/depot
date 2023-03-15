package com.gotocompany.depot.bigtable.parser;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestServiceType;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.utils.MessageConfigUtils;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.model.BigTableSchema;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
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
    private MessageParser mockMessageParser;
    @Mock
    private BigTableRowKeyParser mockBigTableRowKeyParser;
    @Mock
    private ParsedMessage mockParsedMessage;
    private BigTableRecordParser bigTableRecordParser;
    private List<Message> messages;
    private BigTableSinkConfig sinkConfig;

    @Before
    public void setUp() throws IOException, InvalidTemplateException {
        MockitoAnnotations.openMocks(this);
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
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

        Message message1 = new Message(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        Message message2 = new Message(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());
        messages = Collections.list(message1, message2);

        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));

        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidMessages() {
        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidMessagesForComplexFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location\"} }");
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidMessagesForNestedTimestampFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"event_timestamp.nanos\"} }");
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidMessagesForNestedFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location.latitude\"} }");
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnInvalidRecordForAnyNullMessage() {
        List<BigTableRecord> records = bigTableRecordParser.convert(Collections.list(new Message(null, null)));
        assertFalse(records.get(0).isValid());
        assertNotNull(records.get(0).getErrorInfo());
    }

    @Test
    public void shouldCatchEmptyMessageExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsInvalidMessageError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenThrow(EmptyMessageException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.INVALID_MESSAGE_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    @Test
    public void shouldCatchConfigurationExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenThrow(ConfigurationException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    @Test
    public void shouldCatchIOExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsDeserializationError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenThrow(IOException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.DESERIALIZATION_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    @Test
    public void shouldCatchIllegalArgumentExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenReturn(mockParsedMessage);
        when(mockBigTableRowKeyParser.parse(mockParsedMessage)).thenThrow(IllegalArgumentException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
        }
    }
}
