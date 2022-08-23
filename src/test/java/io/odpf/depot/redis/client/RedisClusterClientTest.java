package io.odpf.depot.redis.client;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisHashSetFieldEntry;
import io.odpf.depot.redis.client.entry.RedisKeyValueEntry;
import io.odpf.depot.redis.client.entry.RedisListEntry;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.record.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import io.odpf.depot.redis.util.RedisSinkUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

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
    private RedisClusterClient redisClient;

    @Before
    public void setup() {
        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        records = new ArrayList<>();
    }

    private void populateRedisDataEntry(RedisRecord... redisData) {
        records.addAll(Arrays.asList(redisData));
    }

    @Test
    public void pushesDataEntryForKeyValueInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisCluster.set(key1, value1)).thenReturn("OK");
        when(jedisCluster.set(key2, value2)).thenReturn("OK");

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertTrue(errorInfoMap.isEmpty());
    }

    @Test
    public void reportFailedForKeyValueInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisCluster.set(key1, value1)).thenReturn("OK");
        when(jedisCluster.set(key2, value2)).thenThrow(new JedisException(""));

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertFalse(errorInfoMap.isEmpty());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errorInfoMap.get(2L).getErrorType());
    }

    @Test
    public void setsTTLForKeyValueItemsInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisCluster.set(key1, value1)).thenReturn("OK");
        when(jedisCluster.set(key2, value2)).thenReturn("OK");

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisCluster, key1);
        verify(redisTTL).setTtl(jedisCluster, key2);
    }

    @Test
    public void pushesDataEntryForListInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisCluster.lpush(key1, value1)).thenReturn(1L);
        when(jedisCluster.lpush(key1, value1)).thenReturn(1L);

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertTrue(errorInfoMap.isEmpty());
    }

    @Test
    public void reportsFailedDataEntryForListInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisCluster.lpush(key1, value1)).thenReturn(1L);
        when(jedisCluster.lpush(key2, value2)).thenThrow(new JedisException(""));

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertFalse(errorInfoMap.isEmpty());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errorInfoMap.get(2L).getErrorType());
    }

    @Test
    public void setsTTLForListItemsInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisCluster, key1);
        verify(redisTTL).setTtl(jedisCluster, key2);
    }

    @Test
    public void pushesDataEntryForSetInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisCluster.hset(key1, field1, value1)).thenReturn(0L);
        when(jedisCluster.hset(key2, field2, value2)).thenReturn(1L);

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertTrue(errorInfoMap.isEmpty());
    }

    @Test
    public void reportsFailedDataEntryForSetInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisCluster.hset(key1, field1, value1)).thenReturn(0L);
        when(jedisCluster.hset(key2, field2, value2)).thenThrow(new JedisException(""));

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertFalse(errorInfoMap.isEmpty());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errorInfoMap.get(2L).getErrorType());
    }

    @Test
    public void setsTTLForSetItemsInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisCluster.hset(key1, field1, value1)).thenReturn(0L);
        when(jedisCluster.hset(key1, field1, value1)).thenReturn(0L);

        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisCluster, key1);
        verify(redisTTL).setTtl(jedisCluster, key2);
    }

    @Test
    public void shouldCloseTheClient() {
        redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        redisClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedisCluster, times(1)).close();
    }
}
