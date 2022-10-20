package io.odpf.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestMessage;
import io.odpf.depot.TestNestedMessage;
import io.odpf.depot.TestNestedRepeatedMessage;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisKeyValueEntry;
import io.odpf.depot.redis.enums.RedisSinkDataType;
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
public class RedisKeyValueEntryParserTest {
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
    private RedisEntryParser redisKeyValueEntryParser;
    private OdpfMessageSchema schema;
    private ParsedOdpfMessage parsedOdpfMessage;

    private void redisSinkSetup(String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn(field);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("test-key");
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        String schemaClass = "io.odpf.depot.TestMessage";
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        parsedOdpfMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
        redisKeyValueEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema);
    }

    @Test
    public void shouldConvertParsedOdpfMessageToRedisKeyValueEntry() throws IOException {
        redisSinkSetup("order_details");
        List<RedisEntry> redisDataEntries = redisKeyValueEntryParser.getRedisEntry(parsedOdpfMessage);
        RedisKeyValueEntry expectedEntry = new RedisKeyValueEntry("test-key", "new-eureka-order", null);
        assertEquals(Collections.singletonList(expectedEntry), redisDataEntries);
    }

    @Test
    public void shouldThrowExceptionForInvalidKeyValueDataFieldName() throws IOException {
        redisSinkSetup("test-key", "random-field");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> redisKeyValueEntryParser.getRedisEntry(parsedOdpfMessage));
        assertEquals("Invalid field config : random-field", exception.getMessage());
    }
}
