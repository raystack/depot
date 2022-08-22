package io.odpf.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import io.odpf.depot.*;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.*;
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
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private RedisEntryParser redisKeyValueEntryParser;

    private OdpfMessageSchema schema;
    private ParsedOdpfMessage parsedOdpfMessage;
    private final Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
        put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
        put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
        put(String.format("%s", TestNestedMessage.class.getName()), TestNestedMessage.getDescriptor());
        put(String.format("%s", TestNestedRepeatedMessage.class.getName()), TestNestedRepeatedMessage.getDescriptor());
    }};

    private void redisSinkSetup(String template, String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn(field);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(template);
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        String schemaClass = "io.odpf.depot.TestMessage";
        schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        parsedOdpfMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
        redisKeyValueEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
    }

    @Test
    public void shouldConvertParsedOdpfMessageToRedisKeyValueEntry() throws IOException {
        redisSinkSetup("test-key", "order_details");
        List<RedisEntry> redisDataEntries = redisKeyValueEntryParser.getRedisEntry(parsedOdpfMessage, schema);
        RedisKeyValueEntry expectedEntry = new RedisKeyValueEntry("test-key", "new-eureka-order", null);
        assertEquals(Collections.singletonList(expectedEntry), redisDataEntries);
    }

    @Test
    public void shouldThrowExceptionForInvalidKeyValueDataFieldName() throws IOException {
        redisSinkSetup("test-key", "random-field");
        ConfigurationException configurationException =
                assertThrows(ConfigurationException.class, () -> redisKeyValueEntryParser.getRedisEntry(parsedOdpfMessage, schema));
        assertEquals("Invalid field config : random-field", configurationException.getMessage());
    }
}
