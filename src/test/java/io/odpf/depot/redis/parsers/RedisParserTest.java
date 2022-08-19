package io.odpf.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestLocation;
import io.odpf.depot.TestMessage;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.*;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.models.RedisRecords;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisParserTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StatsDReporter statsDReporter;

    private OdpfMessage message;
    private OdpfMessage bookingMessage;
    private String bookingOrderNumber = "booking-order-1";

    private List<OdpfMessage> messages = new ArrayList<>();

    private Map<String, Descriptors.Descriptor> descriptorsMap;

    @Mock
    private ProtoOdpfMessageParser odpfMessageParser;

    @Before
    public void setUp() throws Exception {
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber(bookingOrderNumber).setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        this.message = new OdpfMessage(testKey.toByteArray(), testMessage.toByteArray());
        this.bookingMessage = new OdpfMessage(testKey.toByteArray(), testBookingLogMessage.toByteArray());

        byte[] message1 = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build().toByteArray();
        byte[] message2 = TestMessage.newBuilder().setOrderNumber("test-order-2").setOrderDetails("ORDER-DETAILS-2").build().toByteArray();
        byte[] message3 = TestMessage.newBuilder().setOrderNumber("test-order-3").setOrderDetails("ORDER-DETAILS-3").build().toByteArray();
        byte[] message4 = TestMessage.newBuilder().setOrderNumber("test-order-4").setOrderDetails("ORDER-DETAILS-4").build().toByteArray();
        byte[] message5 = TestMessage.newBuilder().setOrderNumber("test-order-5").setOrderDetails("ORDER-DETAILS-5").build().toByteArray();

        messages.add(new OdpfMessage(new byte[0], message1));
        messages.add(new OdpfMessage(new byte[0], message2));
        messages.add(new OdpfMessage(new byte[0], message3));
        messages.add(new OdpfMessage(new byte[0], message4));
        messages.add(new OdpfMessage(new byte[0], message5));

        descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
        }};
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() throws IOException {
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        String parsedTemplate = redisParser.parseKeyTemplate("Test-%s,order_number", parsedOdpfMessage, schema);
        Assert.assertEquals("Test-test-order", parsedTemplate);
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() throws IOException {
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        String parsedTemplate = redisParser.parseKeyTemplate("Test-%s, order_number", parsedOdpfMessage, schema);
        Assert.assertEquals("Test-test-order", parsedTemplate);
    }

    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() throws IOException {
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        String parsedTemplate = redisParser.parseKeyTemplate("Test-%.2f,amount_paid_by_cash", parsedOdpfMessage, schema);
        Assert.assertEquals("Test-12.30", parsedTemplate);
    }

    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() throws IOException {
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        String parsedTemplate = redisParser.parseKeyTemplate("Test-%d,customer_total_fare_without_surge", parsedOdpfMessage, schema);
        Assert.assertEquals("Test-2000", parsedTemplate);
    }

    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() throws IOException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template 'null' is invalid");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        redisParser.parseKeyTemplate(null, parsedOdpfMessage, schema);
    }

    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() throws IOException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(bookingMessage, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        redisParser.parseKeyTemplate("", parsedOdpfMessage, schema);
    }

    @Test
    public void shouldAcceptStringForCollectionKey() throws IOException {
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        String parsedTemplate = redisParser.parseKeyTemplate("Test", parsedOdpfMessage, schema);
        Assert.assertEquals("Test", parsedTemplate);
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithEmptyVariables() throws IOException {SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        String parsedTemplate = redisParser.parseKeyTemplate("Test-%s", parsedOdpfMessage, schema);
        Assert.assertEquals("Test-%s", parsedTemplate);
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithMultipleVariables() throws IOException {SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisParser redisParser = new RedisHashSetParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        String parsedTemplate = redisParser.parseKeyTemplate("Test-%s::%s, order_number, order_details", parsedOdpfMessage, schema);
        Assert.assertEquals("Test-test-order::ORDER-DETAILS", parsedTemplate);
    }
}
