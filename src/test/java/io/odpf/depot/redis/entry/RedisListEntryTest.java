package io.odpf.depot.redis.entry;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.ttl.DurationTtl;
import io.odpf.depot.redis.ttl.ExactTimeTtl;
import io.odpf.depot.redis.ttl.NoRedisTtl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class RedisListEntryTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Pipeline pipeline;
    @Mock
    private JedisCluster jedisCluster;
    private RedisListEntry redisListEntry;

    @Before
    public void setup() {
        redisListEntry = new RedisListEntry("test-key", "test-value", instrumentation);
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForPipeline() {
        redisListEntry.send(pipeline, new NoRedisTtl());
        verify(pipeline, times(1)).lpush("test-key", "test-value");
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForPipeline() {
        redisListEntry.send(pipeline, new ExactTimeTtl(1000L));
        verify(pipeline, times(1)).expireAt("test-key", 1000L);
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDurationForPipeline() {
        redisListEntry.send(pipeline, new DurationTtl(1000));
        verify(pipeline, times(1)).expire("test-key", 1000);
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForCluster() {
        redisListEntry.send(jedisCluster, new NoRedisTtl());
        verify(jedisCluster, times(1)).lpush("test-key", "test-value");
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForCluster() {
        redisListEntry.send(jedisCluster, new ExactTimeTtl(1000L));
        verify(jedisCluster, times(1)).expireAt("test-key", 1000L);
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDurationForCluster() {
        redisListEntry.send(jedisCluster, new DurationTtl(1000));
        verify(jedisCluster, times(1)).expire("test-key", 1000);
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldGetListEntryToString() {
        String expected = "RedisListEntry: Key test-key, Value test-value";
        Assert.assertEquals(expected, redisListEntry.toString());
    }
}
