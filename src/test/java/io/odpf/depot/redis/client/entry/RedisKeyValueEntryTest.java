package io.odpf.depot.redis.client.entry;


import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisStandaloneResponse;
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
import redis.clients.jedis.Response;
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
    private InOrder inOrderPipeline;
    private InOrder inOrderJedis;

    @Mock
    private Response<String> response;

    @Before
    public void setup() {
        redisKeyValueEntry = new RedisKeyValueEntry("test-key", "test-value", instrumentation);
        inOrderPipeline = Mockito.inOrder(pipeline);
        inOrderJedis = Mockito.inOrder(jedisCluster);
    }

    @Test
    public void shouldSentToRedisForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertFalse(clusterResponse.isFailed());
        Assert.assertEquals("SET: OK, TTL: NoOp", clusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse redisClusterResponse = redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(redisClusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", redisClusterResponse.getMessage());
    }

    @Test
    public void shouldSetDefaultFailedForPipelineBeforeSync() {
        when(pipeline.set("test-key", "test-value")).thenReturn(response);
        RedisStandaloneResponse sendResponse = redisKeyValueEntry.send(pipeline, new NoRedisTtl());
        Assert.assertTrue(sendResponse.isFailed());
    }

    @Test
    public void shouldSetProperTTLForExactTimeForPipeline() {
        redisKeyValueEntry.send(pipeline, new ExactTimeTtl(1000L));
        inOrderPipeline.verify(pipeline, times(1)).set("test-key", "test-value");
        inOrderPipeline.verify(pipeline, times(1)).expireAt("test-key", 1000L);
    }

    @Test
    public void shouldSetProperTTLForDurationForPipeline() {
        redisKeyValueEntry.send(pipeline, new DurationTtl(1000));
        inOrderPipeline.verify(pipeline, times(1)).set("test-key", "test-value");
        inOrderPipeline.verify(pipeline, times(1)).expire("test-key", 1000);
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForCluster() {
        redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        verify(jedisCluster, times(1)).set("test-key", "test-value");
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
    }

    @Test
    public void shouldSetProperTTLForExactTimeForCluster() {
        redisKeyValueEntry.send(jedisCluster, new ExactTimeTtl(1000L));
        inOrderJedis.verify(jedisCluster, times(1)).set("test-key", "test-value");
        inOrderJedis.verify(jedisCluster, times(1)).expireAt("test-key", 1000L);
    }

    @Test
    public void shouldSetProperTTLForDuration() {
        redisKeyValueEntry.send(jedisCluster, new DurationTtl(1000));
        inOrderJedis.verify(jedisCluster, times(1)).set("test-key", "test-value");
        inOrderJedis.verify(jedisCluster, times(1)).expire("test-key", 1000);
    }

    @Test
    public void shouldGetKeyValueEntryToString() {
        String expected = "RedisKeyValueEntry: Key test-key, Value test-value";
        Assert.assertEquals(expected, redisKeyValueEntry.toString());
    }
}
