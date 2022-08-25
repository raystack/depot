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
public class RedisListEntryTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Pipeline pipeline;
    @Mock
    private JedisCluster jedisCluster;
    private RedisListEntry redisListEntry;

    @Mock
    private Response<Long> response;
    private InOrder inOrderPipeline;
    private InOrder inOrderJedis;

    @Before
    public void setup() {
        inOrderPipeline = Mockito.inOrder(pipeline);
        inOrderJedis = Mockito.inOrder(jedisCluster);
        redisListEntry = new RedisListEntry("test-key", "test-value", instrumentation);
    }

    @Test
    public void shouldSentToRedisForCluster() {
        when(jedisCluster.lpush("test-key", "test-value")).thenReturn(1L);
        RedisClusterResponse redisClusterResponse = redisListEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertFalse(redisClusterResponse.isFailed());
        Assert.assertEquals("LPUSH: 1, TTL: NoOp", redisClusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.lpush("test-key", "test-value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse redisClusterResponse = redisListEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(redisClusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", redisClusterResponse.getMessage());
    }

    @Test
    public void shouldSetDefaultFailedForPipelineBeforeSync() {
        when(pipeline.lpush("test-key", "test-value")).thenReturn(response);
        RedisStandaloneResponse sendResponse = redisListEntry.send(pipeline, new NoRedisTtl());
        Assert.assertTrue(sendResponse.isFailed());
    }

    @Test
    public void shouldSetProperTTLForExactTimeForPipeline() {
        redisListEntry.send(pipeline, new ExactTimeTtl(1000L));
        inOrderPipeline.verify(pipeline, times(1)).lpush("test-key", "test-value");
        inOrderPipeline.verify(pipeline, times(1)).expireAt("test-key", 1000L);
    }

    @Test
    public void shouldSetProperTTLForDurationForPipeline() {
        redisListEntry.send(pipeline, new DurationTtl(1000));
        inOrderPipeline.verify(pipeline, times(1)).lpush("test-key", "test-value");
        inOrderPipeline.verify(pipeline, times(1)).expire("test-key", 1000);
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForCluster() {
        redisListEntry.send(jedisCluster, new NoRedisTtl());
        verify(jedisCluster, times(1)).lpush("test-key", "test-value");
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
    }

    @Test
    public void shouldSetProperTTLForExactTimeForCluster() {
        redisListEntry.send(jedisCluster, new ExactTimeTtl(1000L));
        inOrderJedis.verify(jedisCluster, times(1)).lpush("test-key", "test-value");
        inOrderJedis.verify(jedisCluster, times(1)).expireAt("test-key", 1000L);
    }

    @Test
    public void shouldSetProperTTLForDuration() {
        redisListEntry.send(jedisCluster, new DurationTtl(1000));
        inOrderJedis.verify(jedisCluster, times(1)).lpush("test-key", "test-value");
        inOrderJedis.verify(jedisCluster, times(1)).expire("test-key", 1000);
    }

    @Test
    public void shouldGetKeyValueEntryToString() {
        String expected = "RedisListEntry: Key test-key, Value test-value";
        Assert.assertEquals(expected, redisListEntry.toString());
    }
}
