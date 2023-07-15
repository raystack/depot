package org.raystack.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import org.raystack.depot.TestKey;
import org.raystack.depot.TestMessage;
import org.raystack.depot.TestNestedMessage;
import org.raystack.depot.TestNestedRepeatedMessage;
import org.raystack.depot.config.RedisSinkConfig;
import org.raystack.depot.message.OdpfMessage;
import org.raystack.depot.message.OdpfMessageSchema;
import org.raystack.depot.message.ParsedOdpfMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.proto.ProtoOdpfMessageParser;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.redis.client.entry.RedisListEntry;
import org.raystack.depot.redis.enums.RedisSinkDataType;
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
    private final Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {
        {
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestNestedMessage.class.getName()), TestNestedMessage.getDescriptor());
            put(String.format("%s", TestNestedRepeatedMessage.class.getName()),
                    TestNestedRepeatedMessage.getDescriptor());
        }
    };
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private RedisEntryParser redisListEntryParser;
    private OdpfMessageSchema schema;
    private ParsedOdpfMessage parsedOdpfMessage;

    private void redisSinkSetup(String template, String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisListDataFieldName()).thenReturn(field);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(template);
        ProtoOdpfMessageParser raystackMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        String schemaClass = "org.raystack.depot.TestMessage";
        schema = raystackMessageParser.getSchema(schemaClass, descriptorsMap);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        parsedOdpfMessage = raystackMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
        redisListEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema);
    }

    @Test
    public void shouldConvertParsedOdpfMessageToRedisListEntry() throws IOException {
        redisSinkSetup("test-key", "order_details");
        List<RedisEntry> redisDataEntries = redisListEntryParser.getRedisEntry(parsedOdpfMessage);
        RedisListEntry expectedEntry = new RedisListEntry("test-key", "new-eureka-order", null);
        assertEquals(Collections.singletonList(expectedEntry), redisDataEntries);
    }

    @Test
    public void shouldThrowExceptionForInvalidKeyValueDataFieldName() throws IOException {
        redisSinkSetup("test-key", "random-field");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> redisListEntryParser.getRedisEntry(parsedOdpfMessage));
        assertEquals("Invalid field config : random-field", exception.getMessage());
    }
}
