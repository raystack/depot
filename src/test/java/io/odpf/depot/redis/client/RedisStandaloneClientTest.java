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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneClientTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private Instrumentation instrumentation;
    private final String key1 = "key1";
    private final String key2 = "key2";
    private final String field1 = "field1";
    private final String field2 = "field2";
    private final String value1 = "value1";
    private final String value2 = "value2";

    private final RedisRecord firstKeyValueRecord = new RedisRecord(new RedisKeyValueEntry(key1, value1, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 1L, null, null, true);
    private final RedisRecord secondKeyValueRecord = new RedisRecord(new RedisKeyValueEntry(key2, value2, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 2L, null, null, true);
    private final RedisRecord firstSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key1, field1, value1, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 1L, null, null, true);
    private final RedisRecord secondSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key2, field2, value2, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)), 2L, null, null, true);
    private final RedisRecord firstListRecord = new RedisRecord(new RedisListEntry(key1, value1, new Instrumentation(statsDReporter, RedisListEntry.class)), 1L, null, null, true);
    private final RedisRecord secondListRecord = new RedisRecord(new RedisListEntry(key2, value2, new Instrumentation(statsDReporter, RedisListEntry.class)), 2L, null, null, true);
    private RedisStandaloneClient redisClient;
    private List<RedisRecord> records;
    @Mock
    private RedisTtl redisTTL;

    @Mock
    private Jedis jedis;

    @Mock
    private Pipeline jedisPipeline;

    @Mock
    private Response<List<Object>> responses;

    @Mock
    private Response<String> stringResponseSuccess;

    @Mock
    private Response<String> stringResponseFail;

    @Mock
    private Response<Long> longResponseSuccess;

    @Mock
    private Response<Long> longResponseFail;

    @Before
    public void setUp() {
        records = new ArrayList<>();
        when(jedis.pipelined()).thenReturn(jedisPipeline);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(Collections.singletonList("MOCKED RESPONSE"));
        when(stringResponseSuccess.get()).thenReturn("OK");
        when(stringResponseFail.get()).thenThrow(new JedisException("error while sending"));
        when(longResponseSuccess.get()).thenReturn(0L);
        when(longResponseFail.get()).thenThrow(new JedisException("error while sending"));
    }

    private void populateRedisDataEntry(RedisRecord... redisData) {
        records.addAll(Arrays.asList(redisData));
    }

    @Test
    public void pushesDataEntryForKeyValueInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisPipeline.set(key1, value1)).thenReturn(stringResponseSuccess);
        when(jedisPipeline.set(key2, value2)).thenReturn(stringResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertTrue(errorInfoMap.isEmpty());
    }

    @Test
    public void reportFailedForKeyValueInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisPipeline.set(key1, value1)).thenReturn(stringResponseFail);
        when(jedisPipeline.set(key2, value2)).thenReturn(stringResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertFalse(errorInfoMap.isEmpty());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errorInfoMap.get(1L).getErrorType());
    }

    @Test
    public void setsTTLForKeyValueItemsInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisPipeline.set(key1, value1)).thenReturn(stringResponseSuccess);
        when(jedisPipeline.set(key2, value2)).thenReturn(stringResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void pushesDataEntryForListInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisPipeline.lpush(key1, value1)).thenReturn(longResponseSuccess);
        when(jedisPipeline.lpush(key2, value2)).thenReturn(longResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertTrue(errorInfoMap.isEmpty());
    }

    @Test
    public void reportFailedDataEntryForListInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisPipeline.lpush(key1, value1)).thenReturn(longResponseSuccess);
        when(jedisPipeline.lpush(key2, value2)).thenReturn(longResponseFail);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertFalse(errorInfoMap.isEmpty());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errorInfoMap.get(2L).getErrorType());
    }

    @Test
    public void setsTTLForListItemsInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisPipeline.lpush(key1, value1)).thenReturn(longResponseSuccess);
        when(jedisPipeline.lpush(key2, value2)).thenReturn(longResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void pushesDataEntryForSetInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisPipeline.hset(key1, field1, value1)).thenReturn(longResponseSuccess);
        when(jedisPipeline.hset(key2, field2, value2)).thenReturn(longResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertTrue(errorInfoMap.isEmpty());
    }

    @Test
    public void reportsFailedDataEntryForSetInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisPipeline.hset(key1, field1, value1)).thenReturn(longResponseFail);
        when(jedisPipeline.hset(key2, field2, value2)).thenReturn(longResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        List<RedisResponse> sendResponse = redisClient.send(records);

        Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(records, sendResponse, instrumentation);
        Assert.assertFalse(errorInfoMap.isEmpty());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errorInfoMap.get(1L).getErrorType());
    }

    @Test
    public void setsTTLForSetItemsInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisPipeline.hset(key1, field1, value1)).thenReturn(longResponseSuccess);
        when(jedisPipeline.hset(key2, field2, value2)).thenReturn(longResponseSuccess);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void shouldCloseTheClient() {
        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedis, times(1)).close();
    }
}
