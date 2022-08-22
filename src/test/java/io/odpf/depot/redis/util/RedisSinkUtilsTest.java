package io.odpf.depot.redis.util;

import com.google.protobuf.Descriptors;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestLocation;
import io.odpf.depot.TestMessage;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.message.*;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import static org.junit.Assert.assertEquals;

import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.entry.RedisListEntry;
import io.odpf.depot.redis.record.RedisRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
@RunWith(MockitoJUnitRunner.class)
public class RedisSinkUtilsTest {
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private ParsedOdpfMessage parsedTestMessage;
    private ParsedOdpfMessage parsedBookingMessage;
    private OdpfMessageSchema schemaTest;
    private OdpfMessageSchema schemaBooking;

    @Before
    public void setUp() throws Exception {
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("booking-order-1").setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        OdpfMessage message = new OdpfMessage(testKey.toByteArray(), testMessage.toByteArray());
        OdpfMessage bookingMessage = new OdpfMessage(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
        }};
        String schemaTestClass = "io.odpf.depot.TestMessage";
        String schemaBookingClass = "io.odpf.depot.TestBookingLogMessage";
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        parsedTestMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaTestClass);
        parsedBookingMessage = odpfMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaBookingClass);
        schemaTest = odpfMessageParser.getSchema(schemaTestClass, descriptorsMap);
        schemaBooking = odpfMessageParser.getSchema(schemaBookingClass, descriptorsMap);
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() {
        String parsedTemplate = RedisSinkUtils.parseTemplate("Test-%s,order_number", parsedTestMessage, schemaTest);
        assertEquals("Test-test-order", parsedTemplate);
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() {
        String parsedTemplate = RedisSinkUtils.parseTemplate("Test-%s, order_number", parsedTestMessage, schemaTest);
        assertEquals("Test-test-order", parsedTemplate);
    }

    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() {
        String parsedTemplate = RedisSinkUtils.parseTemplate("Test-%.2f,amount_paid_by_cash", parsedBookingMessage, schemaBooking);
        assertEquals("Test-12.30", parsedTemplate);
    }

    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() {
        String parsedTemplate = RedisSinkUtils.parseTemplate("Test-%d,customer_total_fare_without_surge", parsedBookingMessage, schemaBooking);
        assertEquals("Test-2000", parsedTemplate);
    }

    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> RedisSinkUtils.parseTemplate(null, parsedBookingMessage, schemaBooking));
        assertEquals("Template 'null' is invalid", e.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> RedisSinkUtils.parseTemplate("", parsedBookingMessage, schemaBooking));
        assertEquals("Template '' is invalid", e.getMessage());
    }

    @Test
    public void shouldAcceptStringForCollectionKey() {
        String parsedTemplate = RedisSinkUtils.parseTemplate("Test", parsedBookingMessage, schemaBooking);
        assertEquals("Test", parsedTemplate);
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithEmptyVariables() {
        String parsedTemplate = RedisSinkUtils.parseTemplate("Test-%s", parsedBookingMessage, schemaBooking);
        assertEquals("Test-%s", parsedTemplate);
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithMultipleVariables() {
        String parsedTemplate = RedisSinkUtils.parseTemplate("Test-%s::%s, order_number, order_details", parsedTestMessage, schemaTest);
        assertEquals("Test-test-order::ORDER-DETAILS", parsedTemplate);
    }

    @Test
    public void shouldGetErrorsFromResponse() {
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 7L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 10L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 15L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("FAILED AT 4", true));
        responses.add(new RedisClusterResponse("FAILED AT 7", true));
        responses.add(new RedisClusterResponse("FAILED AT 10", true));
        responses.add(new RedisClusterResponse("OK", false));
        Map<Long, ErrorInfo> errors = RedisSinkUtils.getErrorsFromResponse(records, responses, new Instrumentation(statsDReporter, RedisSinkUtils.class));
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals("FAILED AT 4", errors.get(4L).getException().getMessage());
        Assert.assertEquals("FAILED AT 7", errors.get(7L).getException().getMessage());
        Assert.assertEquals("FAILED AT 10", errors.get(10L).getException().getMessage());
    }
}
