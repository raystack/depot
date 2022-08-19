package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisHashSetFieldEntry;
import io.odpf.depot.redis.dataentry.RedisListEntry;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisClusterClientTest {
    @Mock
    private StatsDReporter statsDReporter;

    private final String key1 = "key1";
    private final String key2 = "key2";
    private final String field1 = "field1";
    private final String field2 = "field2";
    private final String value1 = "value1";
    private final String value2 = "value2";

    @Mock
    private Instrumentation instrumentation;


    private final RedisRecord firstRedisSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key1, field1, value1, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class), 0), 1l, null, null);
    private final RedisRecord secondRedisSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key2, field2, value2, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class), 0), 2l, null, null);

    private final RedisRecord firstRedisListRecord = new RedisRecord(new RedisListEntry(key1, value1, new Instrumentation(statsDReporter, RedisListEntry.class), 0), 1L, null, null);
    private final RedisRecord secondRedisListRecord = new RedisRecord(new RedisListEntry(key2, value2, new Instrumentation(statsDReporter, RedisListEntry.class), 0), 2L, null, null);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private List<RedisRecord> records;
    @Mock
    private RedisTtl redisTTL;
    @Mock
    private JedisCluster jedisCluster;

    private RedisClusterClient redisClusterClient;



    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        redisClusterClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);

        records = new ArrayList<>();
    }

    private void populateRedisDataEntry(RedisRecord... redisData) {
        records.addAll(Arrays.asList(redisData));
    }

    @Test
    public void shouldSendAllListDataWhenExecuting() {
        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
        redisClusterClient.send(records);

        verify(jedisCluster).lpush(key1, value1);
        verify(jedisCluster).lpush(key2, value2);
    }

    @Test
    public void shouldSendAllSetDataWhenExecuting() {
        populateRedisDataEntry(firstRedisSetRecord, secondRedisSetRecord);
        redisClusterClient.send(records);

        verify(jedisCluster).hset(key1, field1, value1);
        verify(jedisCluster).hset(key2, field2, value2);
    }

    @Test
    public void shouldCloseTheJedisClient() {
        redisClusterClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedisCluster).close();
    }
}
