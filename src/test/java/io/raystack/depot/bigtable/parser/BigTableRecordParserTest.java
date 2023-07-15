package org.raystack.depot.bigtable.parser;

import com.google.protobuf.Timestamp;
import org.raystack.depot.TestBookingLogKey;
import org.raystack.depot.TestBookingLogMessage;
import org.raystack.depot.TestLocation;
import org.raystack.depot.TestServiceType;
import org.raystack.depot.bigtable.model.BigTableRecord;
import org.raystack.depot.bigtable.model.BigTableSchema;
import org.raystack.depot.common.Template;
import org.raystack.depot.common.Tuple;
import org.raystack.depot.config.BigTableSinkConfig;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.exception.EmptyMessageException;
import org.raystack.depot.exception.InvalidTemplateException;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.proto.ProtoRaystackMessageParser;
import org.raystack.depot.utils.MessageConfigUtils;
import org.raystack.stencil.client.ClassLoadStencilClient;
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
        private RaystackMessageSchema schema;
        @Mock
        private RaystackMessageParser mockRaystackMessageParser;
        @Mock
        private BigTableRowKeyParser mockBigTableRowKeyParser;
        @Mock
        private ParsedRaystackMessage mockParsedRaystackMessage;
        private BigTableRecordParser bigTableRecordParser;
        private List<RaystackMessage> messages;
        private BigTableSinkConfig sinkConfig;

        @Before
        public void setUp() throws IOException, InvalidTemplateException {
                MockitoAnnotations.openMocks(this);
                System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS",
                                "org.raystack.depot.TestBookingLogMessage");
                System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE",
                                String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
                System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING",
                                "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\"} }");
                System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key-constant-string");

                TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1")
                                .setOrderUrl("order-url#1").build();
                TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1")
                                .setOrderUrl("order-url#1")
                                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100L).setNanos(200).build())
                                .setServiceType(TestServiceType.Enum.GO_SEND)
                                .setDriverPickupLocation(
                                                TestLocation.newBuilder().setLatitude(100D).setLongitude(200D).build())
                                .build();
                TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2")
                                .setOrderUrl("order-url#2").build();
                TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2")
                                .setOrderUrl("order-url#2")
                                .setEventTimestamp(Timestamp.newBuilder().setSeconds(101L).setNanos(202).build())
                                .setServiceType(TestServiceType.Enum.GO_SHOP)
                                .setDriverPickupLocation(
                                                TestLocation.newBuilder().setLatitude(300D).setLongitude(400D).build())
                                .build();

                RaystackMessage message1 = new RaystackMessage(bookingLogKey1.toByteArray(),
                                bookingLogMessage1.toByteArray());
                RaystackMessage message2 = new RaystackMessage(bookingLogKey2.toByteArray(),
                                bookingLogMessage2.toByteArray());
                messages = Collections.list(message1, message2);

                stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
                ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(stencilClient);
                sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
                Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils
                                .getModeAndSchema(sinkConfig);
                BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
                BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(
                                new Template(sinkConfig.getRowKeyTemplate()), schema);

                bigTableRecordParser = new BigTableRecordParser(protoRaystackMessageParser, bigTableRowKeyParser,
                                modeAndSchema,
                                schema, bigtableSchema);
        }

        @Test
        public void shouldReturnValidRecordsForListOfValidRaystackMessages() {
                List<BigTableRecord> records = bigTableRecordParser.convert(messages);
                assertTrue(records.get(0).isValid());
                assertTrue(records.get(1).isValid());
                assertNull(records.get(0).getErrorInfo());
                assertNull(records.get(1).getErrorInfo());
        }

        @Test
        public void shouldReturnValidRecordsForListOfValidRaystackMessagesForComplexFieldsInColumnsMapping()
                        throws InvalidTemplateException {
                System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING",
                                "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location\"} }");
                ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(stencilClient);
                sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
                Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils
                                .getModeAndSchema(sinkConfig);
                BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(
                                new Template(sinkConfig.getRowKeyTemplate()), schema);
                BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
                bigTableRecordParser = new BigTableRecordParser(protoRaystackMessageParser, bigTableRowKeyParser,
                                modeAndSchema,
                                schema, bigtableSchema);

                List<BigTableRecord> records = bigTableRecordParser.convert(messages);
                assertTrue(records.get(0).isValid());
                assertTrue(records.get(1).isValid());
                assertNull(records.get(0).getErrorInfo());
                assertNull(records.get(1).getErrorInfo());
        }

        @Test
        public void shouldReturnValidRecordsForListOfValidRaystackMessagesForNestedTimestampFieldsInColumnsMapping()
                        throws InvalidTemplateException {
                System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING",
                                "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"event_timestamp.nanos\"} }");
                ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(stencilClient);
                sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
                Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils
                                .getModeAndSchema(sinkConfig);
                BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(
                                new Template(sinkConfig.getRowKeyTemplate()), schema);
                BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
                bigTableRecordParser = new BigTableRecordParser(protoRaystackMessageParser, bigTableRowKeyParser,
                                modeAndSchema,
                                schema, bigtableSchema);

                List<BigTableRecord> records = bigTableRecordParser.convert(messages);
                assertTrue(records.get(0).isValid());
                assertTrue(records.get(1).isValid());
                assertNull(records.get(0).getErrorInfo());
                assertNull(records.get(1).getErrorInfo());
        }

        @Test
        public void shouldReturnValidRecordsForListOfValidRaystackMessagesForNestedFieldsInColumnsMapping()
                        throws InvalidTemplateException {
                System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING",
                                "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location.latitude\"} }");
                ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(stencilClient);
                sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
                Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils
                                .getModeAndSchema(sinkConfig);
                BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(
                                new Template(sinkConfig.getRowKeyTemplate()), schema);
                BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
                bigTableRecordParser = new BigTableRecordParser(protoRaystackMessageParser, bigTableRowKeyParser,
                                modeAndSchema,
                                schema, bigtableSchema);

                List<BigTableRecord> records = bigTableRecordParser.convert(messages);
                assertTrue(records.get(0).isValid());
                assertTrue(records.get(1).isValid());
                assertNull(records.get(0).getErrorInfo());
                assertNull(records.get(1).getErrorInfo());
        }

        @Test
        public void shouldReturnInvalidRecordForAnyNullRaystackMessage() {
                List<BigTableRecord> records = bigTableRecordParser
                                .convert(Collections.list(new RaystackMessage(null, null)));
                assertFalse(records.get(0).isValid());
                assertNotNull(records.get(0).getErrorInfo());
        }

        @Test
        public void shouldCatchEmptyMessageExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsInvalidMessageError()
                        throws IOException {
                bigTableRecordParser = new BigTableRecordParser(mockRaystackMessageParser,
                                mockBigTableRowKeyParser,
                                MessageConfigUtils.getModeAndSchema(sinkConfig),
                                schema,
                                new BigTableSchema(sinkConfig.getColumnFamilyMapping()));
                when(mockRaystackMessageParser.parse(any(), any(), any())).thenThrow(EmptyMessageException.class);

                List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

                for (BigTableRecord record : bigTableRecords) {
                        assertFalse(record.isValid());
                        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, record.getErrorInfo().getErrorType());
                }
        }

        @Test
        public void shouldCatchConfigurationExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError()
                        throws IOException {
                bigTableRecordParser = new BigTableRecordParser(mockRaystackMessageParser,
                                mockBigTableRowKeyParser,
                                MessageConfigUtils.getModeAndSchema(sinkConfig),
                                schema,
                                new BigTableSchema(sinkConfig.getColumnFamilyMapping()));
                when(mockRaystackMessageParser.parse(any(), any(), any())).thenThrow(ConfigurationException.class);

                List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

                for (BigTableRecord record : bigTableRecords) {
                        assertFalse(record.isValid());
                        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
                }
        }

        @Test
        public void shouldCatchIOExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsDeserializationError()
                        throws IOException {
                bigTableRecordParser = new BigTableRecordParser(mockRaystackMessageParser,
                                mockBigTableRowKeyParser,
                                MessageConfigUtils.getModeAndSchema(sinkConfig),
                                schema,
                                new BigTableSchema(sinkConfig.getColumnFamilyMapping()));
                when(mockRaystackMessageParser.parse(any(), any(), any())).thenThrow(IOException.class);

                List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

                for (BigTableRecord record : bigTableRecords) {
                        assertFalse(record.isValid());
                        assertEquals(ErrorType.DESERIALIZATION_ERROR, record.getErrorInfo().getErrorType());
                }
        }

        @Test
        public void shouldCatchIllegalArgumentExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError()
                        throws IOException {
                bigTableRecordParser = new BigTableRecordParser(mockRaystackMessageParser,
                                mockBigTableRowKeyParser,
                                MessageConfigUtils.getModeAndSchema(sinkConfig),
                                schema,
                                new BigTableSchema(sinkConfig.getColumnFamilyMapping()));
                when(mockRaystackMessageParser.parse(any(), any(), any())).thenReturn(mockParsedRaystackMessage);
                when(mockBigTableRowKeyParser.parse(mockParsedRaystackMessage))
                                .thenThrow(IllegalArgumentException.class);

                List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

                for (BigTableRecord record : bigTableRecords) {
                        assertFalse(record.isValid());
                        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
                }
        }
}
