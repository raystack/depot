package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisKeyValueEntry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisKeyValueEntryParserTest {
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private RedisEntryParser redisKeyValueEntryParser;
    private ParsedMessage parsedMessage;

    private void redisSinkSetup(String template, String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn(field);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(template);
        ProtoMessageParser messageParser = new ProtoMessageParser(redisSinkConfig, statsDReporter, null);
        String schemaClass = "com.gotocompany.depot.TestMessage";
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage);
        parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
        redisKeyValueEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
    }

    @Test
    public void shouldConvertParsedMessageToRedisKeyValueEntry() throws IOException {
        redisSinkSetup("test-key", "order_details");
        List<RedisEntry> redisDataEntries = redisKeyValueEntryParser.getRedisEntry(parsedMessage);
        RedisKeyValueEntry expectedEntry = new RedisKeyValueEntry("test-key", "new-eureka-order", null);
        assertEquals(Collections.singletonList(expectedEntry), redisDataEntries);
    }

    @Test
    public void shouldThrowExceptionForInvalidKeyValueDataFieldName() throws IOException {
        redisSinkSetup("test-key", "random-field");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> redisKeyValueEntryParser.getRedisEntry(parsedMessage));
        assertEquals("Invalid field config : random-field", exception.getMessage());
    }
}
