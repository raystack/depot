package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
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

    @Test
    public void shouldReturnNewRedisListParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        assertEquals(RedisListEntryParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisHashSetParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        assertEquals(RedisHashSetEntryParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisKeyValueParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        assertEquals(RedisKeyValueEntryParser.class, parser.getClass());
    }
}
