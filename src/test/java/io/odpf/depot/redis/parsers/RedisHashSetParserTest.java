package io.odpf.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import io.odpf.depot.*;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.config.converter.ProtoIndexToFieldMapConverter;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisHashSetFieldEntry;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisHashSetParserTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StatsDReporter statsDReporter;

    private OdpfMessage message;
    private OdpfMessage bookingMessage;
    private String bookingOrderNumber = "booking-order-1";

    private Map<String, Descriptors.Descriptor> descriptorsMap;

    @Before
    public void setUp() throws Exception {

        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber(bookingOrderNumber).setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        this.message = new OdpfMessage(testKey.toByteArray(), testMessage.toByteArray());
        this.bookingMessage = new OdpfMessage(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());

        }};
    }

    private void setRedisSinkConfig(String redisKeyTemplate, String mapping) {
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(redisKeyTemplate);
        Properties properties = new ProtoIndexToFieldMapConverter().convert(null, mapping);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(properties);
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_details\":\"details\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("test-key", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() throws IOException {
        setRedisSinkConfig("Test-%s, order_number", "{\"order_details\":\"details\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() throws IOException {
        setRedisSinkConfig("Test-%.2f,amount_paid_by_cash", "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("Test-12.30", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }
    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() throws IOException {
        setRedisSinkConfig("Test-%d,customer_total_fare_without_surge", "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() throws IOException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template 'null' is invalid");
        setRedisSinkConfig(null, "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
    }

    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() throws IOException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");
        setRedisSinkConfig("", "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
    }

    @Test
    public void shouldAcceptStringForCollectionKey() throws IOException {
        setRedisSinkConfig("Test", "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("Test", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithEmptyVariables() throws IOException {
        setRedisSinkConfig("Test-%s", "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("Test-%s", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_number\":\"ORDER_NUMBER_%d,customer_total_fare_without_surge\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("test-key", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER_2000", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }
    @Test
    public void shouldParseLongMessageWithSpaceForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_number\":\"ORDER_NUMBER_%d, customer_total_fare_without_surge\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("test-key", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER_2000", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }
    @Test
    public void shouldParseStringMessageForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_number\":\"ORDER_NUMBER_%s,order_number\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("test-key", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER_booking-order-1", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldHandleStaticStringForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("test-key", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldHandleStaticStringWithPatternForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_number\":\"ORDER_NUMBER%s\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);

        TestCase.assertEquals("test-key", redisHashSetFieldEntry.getKey());
        TestCase.assertEquals("ORDER_NUMBER%s", redisHashSetFieldEntry.getField());
        TestCase.assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowErrorForInvalidFormatForKey() throws IOException {
        expectedException.expect(UnknownFormatConversionException.class);
        expectedException.expectMessage("Conversion = '%");
        setRedisSinkConfig("test-key", "{\"order_details\":\"ORDER_NUMBER%, order_number\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema);
    }

    //    @Test
//    public void shouldThrowErrorForIncompatibleFormatForKey() {
//        expectedException.expect(IllegalFormatConversionException.class);
//        expectedException.expectMessage("d != java.lang.String");
//
//        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
//        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number-%d,2"));
//        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);
//
//        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(message).get(0);
//
//        TestCase.assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
//        TestCase.assertEquals("details", redisHashSetFieldEntry.getField());
//        TestCase.assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
//    }
    @Test
    public void shouldThrowExceptionForEmptyKey() throws IOException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");
        setRedisSinkConfig("test-key", "{\"order_details\":\"\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema);
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_number\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_KEY;
        String schemaClass = "io.odpf.depot.TestKey";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("ORDER-1-FROM-KEY", redisHashSetFieldEntry.getValue());
        assertEquals("ORDER_NUMBER", redisHashSetFieldEntry.getField());
        assertEquals("test-key", redisHashSetFieldEntry.getKey());
    }

//    @Test(expected = IllegalArgumentException.class)
//    public void shouldThrowInvalidProtocolBufferExceptionWhenIncorrectProtocolUsed() {
//        setRedisSinkConfig("message", "Test-%s,1", RedisSinkDataType.HASHSET);
//        Parser protoParserForTest = stencilClient.getParser(TestNestedRepeatedMessage.class.getCanonicalName());
//        ProtoToFieldMapper protoToFieldMapperForTest = new ProtoToFieldMapper(protoParserForTest, getProperties("3", "details"));
//        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForTest, protoParserForTest, redisSinkConfig, statsDReporter);
//
//        redisMessageParser.parse(message);
//    }
}
