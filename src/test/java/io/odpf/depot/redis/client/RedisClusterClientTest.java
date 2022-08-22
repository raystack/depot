package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisHashSetFieldEntry;
import io.odpf.depot.redis.client.entry.RedisKeyValueEntry;
import io.odpf.depot.redis.client.entry.RedisListEntry;
import io.odpf.depot.redis.record.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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


    private final RedisRecord firstKeyValueRecord = new RedisRecord(new RedisKeyValueEntry(key1, value1, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 1L, null, null, true);
    private final RedisRecord secondKeyValueRecord = new RedisRecord(new RedisKeyValueEntry(key2, value2, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 2L, null, null, true);
    private final RedisRecord firstSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key1, field1, value1, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 1L, null, null, true);
    private final RedisRecord secondSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key2, field2, value2, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 2L, null, null, true);
    private final RedisRecord firstListRecord = new RedisRecord(new RedisListEntry(key1, value1, new Instrumentation(statsDReporter, RedisListEntry.class)), 1L, null, null, true);
    private final RedisRecord secondListRecord = new RedisRecord(new RedisListEntry(key2, value2, new Instrumentation(statsDReporter, RedisListEntry.class)), 2L, null, null, true);

    private List<RedisRecord> records;
    @Mock
    private RedisTtl redisTTL;
    @Mock
    private JedisCluster jedisCluster;
    private RedisClusterClient redisClusterClient;

    @Before
    public void setup() {
        redisClusterClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        records = new ArrayList<>();
    }

    private void populateRedisDataEntry(RedisRecord... redisData) {
        records.addAll(Arrays.asList(redisData));
    }

    @Test
    public void shouldSendKeyValueDataWhenExecuting() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        redisClusterClient.send(records);

        verify(jedisCluster).set(key1, value1);
        verify(jedisCluster).set(key2, value2);
    }
    @Test
    public void shouldSendListDataWhenExecuting() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        redisClusterClient.send(records);

        verify(jedisCluster).lpush(key1, value1);
        verify(jedisCluster).lpush(key2, value2);
    }

    @Test
    public void shouldSendSetDataWhenExecuting() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        redisClusterClient.send(records);

        verify(jedisCluster).hset(key1, field1, value1);
        verify(jedisCluster).hset(key2, field2, value2);
    }

    @Test
    public void shouldSetTTLForKeyValueDataWhenExecuting() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        redisClusterClient.send(records);

        verify(redisTTL).setTtl(jedisCluster, key1);
        verify(redisTTL).setTtl(jedisCluster, key2);
    }

    @Test
    public void shouldSetTTLForListDataWhenExecuting() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        redisClusterClient.send(records);

        verify(redisTTL).setTtl(jedisCluster, key1);
        verify(redisTTL).setTtl(jedisCluster, key2);
    }

    @Test
    public void shouldSetTTLForSetDataWhenExecuting() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        redisClusterClient.send(records);

        verify(redisTTL).setTtl(jedisCluster, key1);
        verify(redisTTL).setTtl(jedisCluster, key2);
    }
    @Test
    public void shouldCloseTheJedisClient() {
        redisClusterClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedisCluster).close();
    }
}
