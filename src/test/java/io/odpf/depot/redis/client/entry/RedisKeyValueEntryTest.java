package io.odpf.depot.redis.client.entry;


import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.ttl.DurationTtl;
import io.odpf.depot.redis.ttl.NoRedisTtl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisKeyValueEntryTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Pipeline pipeline;
    @Mock
    private JedisCluster jedisCluster;
    private RedisKeyValueEntry redisKeyValueEntry;
    private final String key = "key";
    private final String value = "value";
    private InOrder inOrderPipeline;
    private InOrder inOrderJedis;

    @Before
    public void setup() {
        redisKeyValueEntry = new RedisKeyValueEntry(key, value, instrumentation);
        inOrderPipeline = Mockito.inOrder(pipeline);
        inOrderJedis = Mockito.inOrder(jedisCluster);
    }

    @Test
    public void pushMessageWithNoTtl() {
        redisKeyValueEntry.send(pipeline, new NoRedisTtl());
        inOrderPipeline.verify(pipeline, times(1)).set(key, value);
        inOrderPipeline.verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
    }

    @Test
    public void pushMessageWithTtl() {
        redisKeyValueEntry.send(pipeline, new DurationTtl(100));
        inOrderPipeline.verify(pipeline, times(1)).set(key, value);
        inOrderPipeline.verify(pipeline, times(1)).expire(key, 100);
    }

    @Test
    public void pushMessageVerifyInstrumentation() {
        redisKeyValueEntry.send(pipeline, new DurationTtl(100));
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", key, value);
    }

    @Test
    public void pushMessageWithNoTtlUsingJedisCluster() {
        redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        inOrderJedis.verify(jedisCluster, times(1)).set(key, value);
        inOrderJedis.verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
    }

    @Test
    public void pushMessageWithTtlUsingJedisCluster() {
        redisKeyValueEntry.send(jedisCluster, new DurationTtl(100));
        inOrderJedis.verify(jedisCluster, times(1)).set(key, value);
        inOrderJedis.verify(jedisCluster, times(1)).expire(key, 100);
    }
    @Test
    public void pushMessageVerifyInstrumentationUsingJedisCluster() {
        redisKeyValueEntry.send(jedisCluster, new DurationTtl(100));
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", key, value);
    }

    @Test
    public void shouldGetListEntryToString() {
        String expected = "RedisKeyValueEntry: Key key, Value value";
        Assert.assertEquals(expected, redisKeyValueEntry.toString());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.set("key", "value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse response = redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(response.isFailed());
        Assert.assertEquals("jedis error occurred", response.getMessage());
    }
}
