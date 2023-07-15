package org.raystack.depot.redis.parsers;

import org.raystack.depot.config.RedisSinkConfig;
import org.raystack.depot.config.converter.JsonToPropertiesConverter;
import org.raystack.depot.message.MessageSchema;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.redis.enums.RedisSinkDataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisEntryParserFactoryTest {
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private MessageSchema schema;

    @Before
    public void setup() {
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("redis-key");
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn("keyvalue-field");
        when(redisSinkConfig.getSinkRedisListDataFieldName()).thenReturn("list-field");
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, "{\"field\":\"column\"}"));
    }

    @Test
    public void shouldReturnNewRedisListParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema);
        assertEquals(RedisListEntryParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisHashSetParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema);
        assertEquals(RedisHashSetEntryParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisKeyValueParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema);
        assertEquals(RedisKeyValueEntryParser.class, parser.getClass());
    }

    @Test
    public void shouldThrowExceptionForEmptyMappingForHashSet() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, ""));
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema));
        assertEquals("Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found", e.getMessage());
    }

    @Test
    public void shouldThrowExceptionForNullMappingForHashSet() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, null));
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema));
        assertEquals("Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found", e.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyMappingKeyHashSet() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, "{\"order_details\":\"\"}"));
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema));
        assertEquals("Template cannot be empty", e.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyKeyValueDataFieldName() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn("");
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema));
        assertEquals("Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found", illegalArgumentException.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyListDataFieldName() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisListDataFieldName()).thenReturn("");
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema));
        assertEquals("Empty config SINK_REDIS_LIST_DATA_FIELD_NAME found", illegalArgumentException.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyRedisTemplate() {
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("");
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema));
        assertEquals("Template cannot be empty", illegalArgumentException.getMessage());
    }
}
