package io.odpf.depot.redis.client;

import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.entry.RedisHashSetFieldEntry;
import io.odpf.depot.redis.entry.RedisListEntry;
import io.odpf.depot.redis.record.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

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

    private final RedisRecord firstRedisSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key1, field1, value1, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class), 0), 1l, null, null);
    private final RedisRecord secondRedisSetRecord = new RedisRecord(new RedisHashSetFieldEntry(key2, field2, value2, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class), 0), 2l, null, null);

    private final RedisRecord firstRedisListRecord = new RedisRecord(new RedisListEntry(key1, value1, new Instrumentation(statsDReporter, RedisListEntry.class), 0), 1L, null, null);
    private final RedisRecord secondRedisListRecord = new RedisRecord(new RedisListEntry(key2, value2, new Instrumentation(statsDReporter, RedisListEntry.class), 0), 2L, null, null);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private RedisClient redisClient;
    private List<RedisRecord> records;
    @Mock
    private RedisTtl redisTTL;

    @Mock
    private Jedis jedis;

    @Mock
    private Pipeline jedisPipeline;

    @Mock
    private Response<List<Object>> responses;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);

        records = new ArrayList<>();
        when(responses.get()).thenReturn(Collections.singletonList("MOCK_LIST_ITEM"));
        when(jedisPipeline.exec()).thenReturn(responses);
        when(jedis.pipelined()).thenReturn(jedisPipeline);
    }

    private void populateRedisDataEntry(RedisRecord... redisData) {
        records.addAll(Arrays.asList(redisData));
    }

    @Test
    public void pushesDataEntryForListInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
        redisClient.send(records);
        verify(jedisPipeline, times(1)).multi();
        verify(jedisPipeline).lpush(key1, value1);
        verify(jedisPipeline).lpush(key2, value2);
    }

    @Test
    public void setsTTLForListItemsInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
        redisClient.send(records);
        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void pushesDataEntryForSetInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisSetRecord, secondRedisSetRecord);
        redisClient.send(records);
        verify(jedisPipeline, times(1)).multi();
        verify(jedisPipeline).hset(key1, field1, value1);
        verify(jedisPipeline).hset(key2, field2, value2);
    }

    @Test
    public void setsTTLForSetItemsInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisSetRecord, secondRedisSetRecord);
        redisClient.send(records);
        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void shouldCompleteTransaction() {
        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
        redisClient.send(records);
        verify(jedisPipeline).exec();
        verify(instrumentation, times(1)).logDebug("jedis responses: {}", responses);
    }

    @Test
    public void shouldWaitForResponseInExec() {
        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
        redisClient.send(records);
        verify(jedisPipeline).sync();
    }

//    @Test
//    public void shouldThrowExceptionWhenResponseIsNullInExec() {
//        expectedException.expect(NoResponseException.class);
//
//        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
//        when(jedisPipeline.exec()).thenReturn(responses);
//        when(responses.get()).thenReturn(null);
//
//        redisClient.execute(records);
//    }

//    @Test
//    public void shouldThrowExceptionWhenResponseIsEmptyInExec() {
//        expectedException.expect(NoResponseException.class);
//
//        populateRedisDataEntry(firstRedisListRecord, secondRedisListRecord);
//        when(jedisPipeline.exec()).thenReturn(responses);
//        when(responses.get()).thenReturn(new ArrayList<>());
//
//        redisClient.execute(records);
//    }


//    @Test
//    public void shouldCloseTheClient() {
//        redisClient.close();
//
//        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
//        verify(jedis, times(1)).close();
//    }
}
