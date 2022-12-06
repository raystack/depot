package io.odpf.depot.redis.parsers;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestLocation;
import io.odpf.depot.TestMessageBQ;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.config.converter.JsonToPropertiesConverter;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisHashSetFieldEntry;
import io.odpf.depot.redis.enums.RedisSinkDataType;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisHashSetEntryParserTest {
    private final Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
        put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
        put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
        put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
        put(String.format("%s", TestBookingLogMessage.TopicMetadata.class.getName()), TestBookingLogMessage.TopicMetadata.getDescriptor());
        put(String.format("%s", TestMessageBQ.class.getName()), TestMessageBQ.getDescriptor());
        put("io.odpf.depot.TestMessageBQ.CurrentStateEntry", TestMessageBQ.getDescriptor().getNestedTypes().get(0));
        put("com.google.protobuf.Struct.FieldsEntry", Struct.getDescriptor().getNestedTypes().get(0));
        put("com.google.protobuf.Duration", com.google.protobuf.Duration.getDescriptor());
        put("com.google.type.Date", com.google.type.Date.getDescriptor());
    }};
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private ParsedOdpfMessage parsedBookingMessage;
    private ParsedOdpfMessage parsedOdpfKey;
    private OdpfMessageSchema schemaBooking;
    private OdpfMessageSchema schemaKey;

    private void redisSinkSetup(String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, field));
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("test-key");
        String schemaBookingClass = "io.odpf.depot.TestBookingLogMessage";
        String schemaKeyClass = "io.odpf.depot.TestKey";
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("booking-order-1").setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        OdpfMessage bookingMessage = new OdpfMessage(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        parsedBookingMessage = odpfMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaBookingClass);
        parsedOdpfKey = odpfMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_KEY, schemaKeyClass);
        schemaBooking = odpfMessageParser.getSchema(schemaBookingClass, descriptorsMap);
        schemaKey = odpfMessageParser.getSchema(schemaKeyClass, descriptorsMap);
    }

    @Test
    public void shouldParseComplexProtoType() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"topics\":\"topics_%s,customer_name\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:driver:%s,customer_name"
        ));
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(config, statsDReporter, null);
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(123)
                        .setTopic("topic2").build())
                .build();
        OdpfMessage bookingMessage = new OdpfMessage(null, testBookingLogMessage.toByteArray());
        String schemaMessageClass = "io.odpf.depot.TestBookingLogMessage";
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaMessageClass, descriptorsMap);

        parsedBookingMessage = odpfMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaMessageClass);

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter, schema);
        List<RedisEntry> redisEntry = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        assertEquals(1, redisEntry.size());
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisEntry.get(0);
        assertEquals("subscription:driver:johndoe", redisHashSetFieldEntry.getKey());
        assertEquals("topics_johndoe", redisHashSetFieldEntry.getField());
        assertEquals(new JSONArray("[{\"qos\":1,\"topic\":\"hellowo/rl/dcom.world.partner\"},{\"qos\":123,\"topic\":\"topic2\"}]").toString(),
                new JSONArray(redisHashSetFieldEntry.getValue()).toString());
    }

    @Test
    public void shouldParseRepeatedStruct() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"attributes\":\"test_order_%s,created_at\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:order:%s,order_number"
        ));
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(config, statsDReporter, null);
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(60).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("active", Value.newBuilder().setBoolValue(true).build())
                        .putFields("height", Value.newBuilder().setNumberValue(175).build()).build())
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1669433359).build())
                .setOrderNumber("test_order")
                .build();

        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        OdpfMessage odpfMessage = new OdpfMessage(null, message.toByteArray());
        ParsedOdpfMessage parsedMessage = odpfMessageParser.parse(odpfMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "io.odpf.depot.TestMessageBQ");

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter, odpfMessageSchema);
        List<RedisEntry> redisEntry = redisHashSetEntryParser.getRedisEntry(parsedMessage);
        assertEquals(1, redisEntry.size());
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisEntry.get(0);
        assertEquals("subscription:order:test_order", redisHashSetFieldEntry.getKey());
        assertEquals("test_order_2022-11-26T03:29:19Z", redisHashSetFieldEntry.getField());
        assertEquals("[{\"name\":\"John\",\"age\":50.0},{\"name\":\"John\",\"age\":60.0},{\"name\":\"John\",\"active\":true,\"height\":175.0}]",
                redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowExceptionForWrongConfig() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"does_not_exist\":\"test_order_%s,order_number\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:order:%s,order_number"
        ));

        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(config, statsDReporter, null);
        TestMessageBQ message = TestMessageBQ.newBuilder().setOrderNumber("test").build();
        OdpfMessageSchema odpfMessageSchema = odpfMessageParser.getSchema("io.odpf.depot.TestMessageBQ", descriptorsMap);
        OdpfMessage odpfMessage = new OdpfMessage(null, message.toByteArray());
        ParsedOdpfMessage parsedMessage = odpfMessageParser.parse(odpfMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "io.odpf.depot.TestMessageBQ");

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter, odpfMessageSchema);
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> redisHashSetEntryParser.getRedisEntry(parsedMessage));
        Assert.assertEquals("Invalid field config : does_not_exist", exception.getMessage());
    }

    @Test
    public void shouldParseLongMessageForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER_%s,customer_total_fare_without_surge\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schemaBooking);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER_2000", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    @Test
    public void shouldParseLongMessageWithSpaceForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER_%s, customer_total_fare_without_surge\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schemaBooking);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER_2000", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    @Test
    public void shouldParseStringMessageForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER_%s,order_number\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schemaBooking);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER_booking-order-1", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    @Test
    public void shouldHandleStaticStringForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schemaBooking);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    @Test
    public void shouldThrowErrorForInvalidFormatForKey() throws IOException {
        redisSinkSetup("{\"order_details\":\"ORDER_NUMBER%, order_number\"}");
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schemaBooking));
        assertEquals("Template is not valid, variables=1, validArgs=0, values=1", e.getMessage());
    }

    @Test
    public void shouldThrowErrorForIncompatibleFormatForKey() throws IOException {
        redisSinkSetup("{\"order_details\":\"order_number-%d, order_number\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schemaBooking);
        IllegalFormatConversionException e = Assert.assertThrows(IllegalFormatConversionException.class,
                () -> redisHashSetEntryParser.getRedisEntry(parsedBookingMessage));
        assertEquals("d != java.lang.String", e.getMessage());
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schemaKey);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedOdpfKey);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER", "ORDER-1-FROM-KEY", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }
}
