package com.gotocompany.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisListEntry;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestNestedMessage;
import com.gotocompany.depot.TestNestedRepeatedMessage;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisListEntryParserTest {
    private final Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
        put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
        put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
        put(String.format("%s", TestNestedMessage.class.getName()), TestNestedMessage.getDescriptor());
        put(String.format("%s", TestNestedRepeatedMessage.class.getName()), TestNestedRepeatedMessage.getDescriptor());
    }};
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private RedisEntryParser redisListEntryParser;
    private MessageSchema schema;
    private ParsedMessage parsedMessage;

    private void redisSinkSetup(String template, String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisListDataFieldName()).thenReturn(field);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(template);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(redisSinkConfig, statsDReporter, null);
        String schemaClass = "com.gotocompany.depot.TestMessage";
        schema = protoMessageParser.getSchema(schemaClass, descriptorsMap);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage);
        parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
        redisListEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema);
    }

    @Test
    public void shouldConvertParsedMessageToRedisListEntry() throws IOException {
        redisSinkSetup("test-key", "order_details");
        List<RedisEntry> redisDataEntries = redisListEntryParser.getRedisEntry(parsedMessage);
        RedisListEntry expectedEntry = new RedisListEntry("test-key", "new-eureka-order", null);
        assertEquals(Collections.singletonList(expectedEntry), redisDataEntries);
    }

    @Test
    public void shouldThrowExceptionForInvalidKeyValueDataFieldName() throws IOException {
        redisSinkSetup("test-key", "random-field");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> redisListEntryParser.getRedisEntry(parsedMessage));
        assertEquals("Invalid field config : random-field", exception.getMessage());
    }
}
