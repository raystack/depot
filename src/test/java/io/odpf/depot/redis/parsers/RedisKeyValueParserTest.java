package io.odpf.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import io.odpf.depot.*;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.*;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisKeyValueEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisKeyValueParserTest {
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;

    private Map<String, Descriptors.Descriptor> descriptorsMap;

    @Before
    public void setup() {
        descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestNestedMessage.class.getName()), TestNestedMessage.getDescriptor());
            put(String.format("%s", TestNestedRepeatedMessage.class.getName()), TestNestedRepeatedMessage.getDescriptor());
        }};
    }

    private void setRedisSinkConfig(String template, String field) {
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn(field);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(template);
    }

    @Test
    public void shouldConvertParsedOdpfMessageToRedisKeyValueEntry() throws IOException {
        setRedisSinkConfig("test-key", "order_details");
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisKeyValueParser redisKeyValueParser = new RedisKeyValueParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        List<RedisDataEntry> redisDataEntries = redisKeyValueParser.getRedisEntry(parsedOdpfMessage, schema);
        RedisKeyValueEntry expectedEntry = new RedisKeyValueEntry("test-key", "new-eureka-order", null);
        assertEquals(asList(expectedEntry), redisDataEntries);
    }

    @Test
    public void shouldThrowExceptionForEmptyKeyValueDataFieldName() throws IOException {
        setRedisSinkConfig("test-key", "");
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisKeyValueParser redisKeyValueParser = new RedisKeyValueParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> redisKeyValueParser.getRedisEntry(parsedOdpfMessage, schema));
        assertEquals("Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found", illegalArgumentException.getMessage());
    }

    @Test
    public void shouldThrowExceptionForInvalidKeyValueDataFieldName() throws IOException {
        setRedisSinkConfig("test-key", "random-field");
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        RedisKeyValueParser redisKeyValueParser = new RedisKeyValueParser(odpfMessageParser, redisSinkConfig, statsDReporter);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        SinkConnectorSchemaMessageMode mode = SinkConnectorSchemaMessageMode.LOG_MESSAGE;
        String schemaClass = "io.odpf.depot.TestMessage";
        OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass, descriptorsMap);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
        ConfigurationException configurationException =
                assertThrows(ConfigurationException.class, () -> redisKeyValueParser.getRedisEntry(parsedOdpfMessage, schema));
        assertEquals("Invalid field config : random-field", configurationException.getMessage());
    }
}
