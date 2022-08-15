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
public class RedisParserFactoryTest {
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

        RedisParser parser = RedisParserFactory.getParser(redisSinkConfig, statsDReporter);

        assertEquals(RedisListParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisHashSetParser() {
        setRedisSinkConfig(RedisSinkDataType.HASHSET, SinkConnectorSchemaDataType.PROTOBUF);

        RedisParser parser = RedisParserFactory.getParser(redisSinkConfig, statsDReporter);

        assertEquals(RedisHashSetParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisKeyValueParser() {
        setRedisSinkConfig(RedisSinkDataType.KEYVALUE, SinkConnectorSchemaDataType.PROTOBUF);

        RedisParser parser = RedisParserFactory.getParser(redisSinkConfig, statsDReporter);

        assertEquals(RedisKeyValueParser.class, parser.getClass());
    }
}
