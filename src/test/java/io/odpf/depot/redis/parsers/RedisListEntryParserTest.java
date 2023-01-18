package io.odpf.depot.redis.parsers;

import io.odpf.depot.TestMessage;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisListEntry;
import io.odpf.depot.redis.enums.RedisSinkDataType;
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
public class RedisListEntryParserTest {
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
        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(redisSinkConfig, statsDReporter, null);
        String schemaClass = "io.odpf.depot.TestMessage";
        schema = null;
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        parsedOdpfMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
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
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> redisListEntryParser.getRedisEntry(parsedOdpfMessage));
        assertEquals("Invalid field config : random-field", exception.getMessage());
    }
}
