package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.enums.RedisSinkDataType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class RedisEntryParserFactoryTest {
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;

    private void setRedisSinkConfig(RedisSinkDataType redisSinkDataType, SinkConnectorSchemaDataType sinkConnectorSchemaDataType) {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(redisSinkDataType);
        when(redisSinkConfig.getSinkConnectorSchemaDataType()).thenReturn(sinkConnectorSchemaDataType);
    }

    @Test
    public void shouldReturnNewRedisListParser() {
        setRedisSinkConfig(RedisSinkDataType.LIST, SinkConnectorSchemaDataType.PROTOBUF);

        RedisParser parser = RedisEntryParserFactory.getParser(redisSinkConfig, statsDReporter);

        assertEquals(RedisListEntryParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisHashSetParser() {
        setRedisSinkConfig(RedisSinkDataType.HASHSET, SinkConnectorSchemaDataType.PROTOBUF);

        RedisParser parser = RedisEntryParserFactory.getParser(redisSinkConfig, statsDReporter);

        assertEquals(RedisHashSetEntryParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisKeyValueParser() {
        setRedisSinkConfig(RedisSinkDataType.KEYVALUE, SinkConnectorSchemaDataType.PROTOBUF);

        RedisParser parser = RedisEntryParserFactory.getParser(redisSinkConfig, statsDReporter);

        assertEquals(RedisKeyValueEntryParser.class, parser.getClass());
    }
}
