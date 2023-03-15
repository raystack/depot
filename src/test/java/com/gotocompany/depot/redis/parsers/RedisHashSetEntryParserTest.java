package com.gotocompany.depot.redis.parsers;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.config.converter.JsonToPropertiesConverter;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisHashSetFieldEntry;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.IllegalFormatConversionException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisHashSetEntryParserTest {
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private ParsedMessage parsedBookingMessage;
    private ParsedMessage parsedKey;
    private MessageSchema schemaBooking;
    private MessageSchema schemaKey;

    private void redisSinkSetup(String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, field));
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("test-key");
        String schemaBookingClass = "com.gotocompany.depot.TestBookingLogMessage";
        String schemaKeyClass = "com.gotocompany.depot.TestKey";
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("booking-order-1").setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        Message bookingMessage = new Message(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(redisSinkConfig, statsDReporter, null);
        parsedBookingMessage = protoMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaBookingClass);
        parsedKey = protoMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_KEY, schemaKeyClass);
        schemaBooking = null;
        schemaKey = null;
    }

    @Test
    public void shouldParseComplexProtoType() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"topics\":\"topics_%s,customer_name\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:driver:%s,customer_name"
        ));
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(config, statsDReporter, null);
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(123)
                        .setTopic("topic2").build())
                .build();
        Message bookingMessage = new Message(null, testBookingLogMessage.toByteArray());
        String schemaMessageClass = "com.gotocompany.depot.TestBookingLogMessage";
        MessageSchema schema = null;

        parsedBookingMessage = protoMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaMessageClass);

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
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(config, statsDReporter, null);
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

        MessageSchema messageSchema = null;
        Message message1 = new Message(null, message.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message1, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessageBQ");

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter, messageSchema);
        List<RedisEntry> redisEntry = redisHashSetEntryParser.getRedisEntry(parsedMessage);
        assertEquals(1, redisEntry.size());
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisEntry.get(0);
        assertEquals("subscription:order:test_order", redisHashSetFieldEntry.getKey());
        assertEquals("test_order_2022-11-26T03:29:19Z", redisHashSetFieldEntry.getField());
        assertEquals("[{\"name\":\"John\",\"age\":50},{\"name\":\"John\",\"age\":60},{\"name\":\"John\",\"active\":true,\"height\":175}]",
                redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowExceptionForWrongConfig() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"does_not_exist\":\"test_order_%s,order_number\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:order:%s,order_number"
        ));

        ProtoMessageParser protoMessageParser = new ProtoMessageParser(config, statsDReporter, null);
        TestMessageBQ message = TestMessageBQ.newBuilder().setOrderNumber("test").build();
        MessageSchema messageSchema = null;
        Message message1 = new Message(null, message.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message1, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessageBQ");

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter, messageSchema);
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
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedKey);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER", "ORDER-1-FROM-KEY", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }
}
