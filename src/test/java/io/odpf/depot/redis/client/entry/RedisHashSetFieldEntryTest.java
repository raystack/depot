package io.odpf.depot.redis.client.entry;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.ttl.DurationTtl;
import io.odpf.depot.redis.ttl.ExactTimeTtl;
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
public class RedisHashSetFieldEntryTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Pipeline pipeline;
    @Mock
    private JedisCluster jedisCluster;
    private RedisHashSetFieldEntry redisHashSetFieldEntry;
    private InOrder inOrderPipeline;
    private InOrder inOrderJedis;

    @Before
    public void setup() {
        redisHashSetFieldEntry = new RedisHashSetFieldEntry("test-key", "test-field", "test-value", instrumentation);
        inOrderPipeline = Mockito.inOrder(pipeline);
        inOrderJedis = Mockito.inOrder(jedisCluster);
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForPipeline() {
        redisHashSetFieldEntry.send(pipeline, new NoRedisTtl());
        verify(pipeline, times(1)).hset("test-key", "test-field", "test-value");
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForPipeline() {
        redisHashSetFieldEntry.send(pipeline, new ExactTimeTtl(1000L));
        inOrderPipeline.verify(pipeline, times(1)).hset("test-key", "test-field", "test-value");
        inOrderPipeline.verify(pipeline, times(1)).expireAt("test-key", 1000L);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDurationForPipeline() {
        redisHashSetFieldEntry.send(pipeline, new DurationTtl(1000));
        inOrderPipeline.verify(pipeline, times(1)).hset("test-key", "test-field", "test-value");
        inOrderPipeline.verify(pipeline, times(1)).expire("test-key", 1000);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForCluster() {
        redisHashSetFieldEntry.send(jedisCluster, new NoRedisTtl());
        verify(jedisCluster, times(1)).hset("test-key", "test-field", "test-value");
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForCluster() {
        redisHashSetFieldEntry.send(jedisCluster, new ExactTimeTtl(1000L));
        inOrderJedis.verify(jedisCluster, times(1)).hset("test-key", "test-field", "test-value");
        inOrderJedis.verify(jedisCluster, times(1)).expireAt("test-key", 1000L);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDuration() {
        redisHashSetFieldEntry.send(jedisCluster, new DurationTtl(1000));
        inOrderJedis.verify(jedisCluster, times(1)).hset("test-key", "test-field", "test-value");
        inOrderJedis.verify(jedisCluster, times(1)).expire("test-key", 1000);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldGetSetEntryToString() {
        String expected = "RedisHashSetFieldEntry Key test-key, Field test-field, Value test-value";
        Assert.assertEquals(expected, redisHashSetFieldEntry.toString());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.hset("test-key", "test-field", "test-value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse response = redisHashSetFieldEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(response.isFailed());
        Assert.assertEquals("jedis error occurred", response.getMessage());
    }
}
