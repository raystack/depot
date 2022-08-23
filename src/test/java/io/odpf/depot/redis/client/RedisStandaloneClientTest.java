package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisHashSetFieldEntry;
import io.odpf.depot.redis.client.entry.RedisKeyValueEntry;
import io.odpf.depot.redis.client.entry.RedisListEntry;
import io.odpf.depot.redis.record.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Builder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
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

    private final Response<Long> longResponse = new Response<>(new Builder<Long>() {
        @Override
        public Long build(Object data) {
            return 0L;
        }
    });

    private final Response<String> stringResponse = new Response<>(new Builder<String>() {
        @Override
        public String build(Object data) {
            return "OK";
        }
    });

    @Before
    public void setUp() {
        records = new ArrayList<>();
        when(jedis.pipelined()).thenReturn(jedisPipeline);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(Collections.singletonList("MOCKED RESPONSE"));
    }

    private void populateRedisDataEntry(RedisRecord... redisData) {
        records.addAll(Arrays.asList(redisData));
    }

    @Test
    public void pushesDataEntryForKeyValueInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisPipeline.set(key1, value1)).thenReturn(stringResponse);
        when(jedisPipeline.set(key2, value2)).thenReturn(stringResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(jedisPipeline, times(1)).multi();
        verify(jedisPipeline).set(key1, value1);
        verify(jedisPipeline).set(key2, value2);
    }

    @Test
    public void setsTTLForKeyValueItemsInATransaction() {
        populateRedisDataEntry(firstKeyValueRecord, secondKeyValueRecord);
        when(jedisPipeline.set(key1, value1)).thenReturn(stringResponse);
        when(jedisPipeline.set(key2, value2)).thenReturn(stringResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void pushesDataEntryForListInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisPipeline.lpush(key1, value1)).thenReturn(longResponse);
        when(jedisPipeline.lpush(key2, value2)).thenReturn(longResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(jedisPipeline, times(1)).multi();
        verify(jedisPipeline).lpush(key1, value1);
        verify(jedisPipeline).lpush(key2, value2);
    }

    @Test
    public void setsTTLForListItemsInATransaction() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisPipeline.lpush(key1, value1)).thenReturn(longResponse);
        when(jedisPipeline.lpush(key2, value2)).thenReturn(longResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void pushesDataEntryForSetInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisPipeline.hset(key1, field1, value1)).thenReturn(longResponse);
        when(jedisPipeline.hset(key2, field2, value2)).thenReturn(longResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(jedisPipeline, times(1)).multi();
        verify(jedisPipeline).hset(key1, field1, value1);
        verify(jedisPipeline).hset(key2, field2, value2);
    }

    @Test
    public void setsTTLForSetItemsInATransaction() {
        populateRedisDataEntry(firstSetRecord, secondSetRecord);
        when(jedisPipeline.hset(key1, field1, value1)).thenReturn(longResponse);
        when(jedisPipeline.hset(key2, field2, value2)).thenReturn(longResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);
    }

    @Test
    public void shouldCompleteTransactionInSend() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisPipeline.lpush(key1, value1)).thenReturn(longResponse);
        when(jedisPipeline.lpush(key2, value2)).thenReturn(longResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);

        verify(jedisPipeline, times(1)).exec();
    }

    @Test
    public void shouldWaitForResponseInExec() {
        populateRedisDataEntry(firstListRecord, secondListRecord);
        when(jedisPipeline.lpush(key1, value1)).thenReturn(longResponse);
        when(jedisPipeline.lpush(key2, value2)).thenReturn(longResponse);

        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.send(records);

        verify(redisTTL).setTtl(jedisPipeline, key1);
        verify(redisTTL).setTtl(jedisPipeline, key2);

        verify(jedisPipeline).sync();
    }


    @Test
    public void shouldCloseTheClient() {
        redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedis, times(1)).close();
    }
}
