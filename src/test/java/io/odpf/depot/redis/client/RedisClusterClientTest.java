package io.odpf.depot.redis.client;

import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisHashSetFieldEntry;
import io.odpf.depot.redis.dataentry.RedisListEntry;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
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

    @Mock
    private Instrumentation instrumentation;

    private final RedisRecord firstRedisSetRecord = new RedisRecord(new RedisHashSetFieldEntry("key1", "field1", "value1", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 1L, null);
    private final RedisRecord secondRedisSetRecord = new RedisRecord(new RedisHashSetFieldEntry("key2", "field2", "value2", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 2L, null);

    private final RedisRecord firstRedisListRecord = new RedisRecord(new RedisListEntry("key1", "value1", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 1L, null);
    private final RedisRecord secondRedisListRecord = new RedisRecord(new RedisListEntry("key2", "value2", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 2L, null);

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
        redisClusterClient.execute(records);

        verify(jedisCluster).lpush(((RedisListEntry) firstRedisListRecord.getRedisDataEntry()).getKey(), ((RedisListEntry) firstRedisListRecord.getRedisDataEntry()).getValue());
        verify(jedisCluster).lpush(((RedisListEntry) secondRedisListRecord.getRedisDataEntry()).getKey(), ((RedisListEntry) secondRedisListRecord.getRedisDataEntry()).getValue());
    }

    @Test
    public void shouldSendAllSetDataWhenExecuting() {
        populateRedisDataEntry(firstRedisSetRecord, secondRedisSetRecord);
        redisClusterClient.execute(records);

        verify(jedisCluster).hset(((RedisHashSetFieldEntry) firstRedisSetRecord.getRedisDataEntry()).getKey(), ((RedisHashSetFieldEntry) firstRedisSetRecord.getRedisDataEntry()).getField(), ((RedisHashSetFieldEntry) firstRedisSetRecord.getRedisDataEntry()).getValue());
        verify(jedisCluster).hset(((RedisHashSetFieldEntry) secondRedisSetRecord.getRedisDataEntry()).getKey(), ((RedisHashSetFieldEntry) secondRedisSetRecord.getRedisDataEntry()).getField(), ((RedisHashSetFieldEntry) secondRedisSetRecord.getRedisDataEntry()).getValue());
    }

    @Test
    public void shouldReturnEmptyArrayAfterExecuting() {
        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
        List<OdpfMessage> retryElements = redisClusterClient.execute(records);

        Assert.assertEquals(0, retryElements.size());
    }

    @Test
    public void shouldCloseTheJedisClient() {
        redisClusterClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedisCluster).close();
    }
}
