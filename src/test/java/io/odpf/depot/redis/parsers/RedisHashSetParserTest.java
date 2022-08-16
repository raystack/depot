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
    private String bookingOrderNumber = "booking-order-1";

    private Map<String, Descriptors.Descriptor> descriptorsMap;

    @Before
    public void setUp() throws Exception {

        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        this.message = new OdpfMessage(testKey.toByteArray(), testMessage.toByteArray());
        descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
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
    public void shouldParseLongMessageForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_details\":\"ORDER_%s,order_number\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("ORDER_test-order", redisHashSetFieldEntry.getField());
        assertEquals("test-key", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldParseLongMessageWithSpaceForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_details\":\"ORDER_%s, order_number\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("ORDER_test-order", redisHashSetFieldEntry.getField());
        assertEquals("test-key", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldHandleStaticStringForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_details\":\"ORDER_NUMBER\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("ORDER_NUMBER", redisHashSetFieldEntry.getField());
        assertEquals("test-key", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldHandleStaticStringWithPatternForKey() throws IOException {
        setRedisSinkConfig("test-key", "{\"order_details\":\"ORDER_NUMBER%s\"}");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("ORDER_NUMBER%s", redisHashSetFieldEntry.getField());
        assertEquals("test-key", redisHashSetFieldEntry.getKey());
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
//    public void shouldThrowErrorForIncompatibleFormatForKey() throws IOException {
//        expectedException.expect(IllegalFormatConversionException.class);
//        expectedException.expectMessage("d != java.lang.String");
//
//        expectedException.expect(UnknownFormatConversionException.class);
//        expectedException.expectMessage("Conversion = '%");
//        setRedisSinkConfig("test-key", "{\"order_details\":\"ORDER_NUMBER-%d,order_number\"}");
//        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
//        String schemaClass = "io.odpf.depot.TestMessage";
//        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
//        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
//        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
//        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
//        redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema);
//    }
//
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
//    public void shouldThrowInvalidProtocolBufferExceptionWhenIncorrectProtocolUsed() throws IOException {
//        setRedisSinkConfig("test-key", "{\"order_details\":\"details\"}");
//        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
//        String schemaClass = "io.odpf.depot.TestNestedMessage";
//        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
//        RedisParser redisMessageParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
//        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
//        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
//        redisMessageParser.parseRedisEntry(parsedOdpfMessage, schema).get(0);
//    }
}
