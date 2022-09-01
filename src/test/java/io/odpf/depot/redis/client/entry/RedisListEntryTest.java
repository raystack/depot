package io.odpf.depot.redis.client.entry;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisStandaloneResponse;
import io.odpf.depot.redis.ttl.DurationTtl;
import io.odpf.depot.redis.ttl.NoRedisTtl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

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

    @Before
    public void setup() {
        redisListEntry = new RedisListEntry("test-key", "test-value", instrumentation);
    }

    @Test
    public void shouldSentToRedisForCluster() {
        when(jedisCluster.lpush("test-key", "test-value")).thenReturn(9L);
        RedisClusterResponse clusterResponse = redisListEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("LPUSH: 9, TTL: NoOp", clusterResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForClusterWithTTL() {
        when(jedisCluster.lpush("test-key", "test-value")).thenReturn(9L);
        when(jedisCluster.expire("test-key", 1000)).thenReturn(1L);
        RedisClusterResponse clusterResponse = redisListEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("LPUSH: 9, TTL: UPDATED", clusterResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForClusterWithTTLNotUpdated() {
        when(jedisCluster.lpush("test-key", "test-value")).thenReturn(9L);
        when(jedisCluster.expire("test-key", 1000)).thenReturn(0L);
        RedisClusterResponse clusterResponse = redisListEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("LPUSH: 9, TTL: NOT UPDATED", clusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.lpush("test-key", "test-value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisListEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionFromTTLForCluster() {
        when(jedisCluster.lpush("test-key", "test-value")).thenReturn(9L);
        when(jedisCluster.expire("test-key", 1000)).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisListEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    @Test
    public void shouldGetEntryToString() {
        String expected = "RedisListEntry: Key test-key, Value test-value";
        Assert.assertEquals(expected, redisListEntry.toString());
    }


    @Test
    public void shouldSentToRedisForStandAlone() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn(9L);
        when(pipeline.lpush("test-key", "test-value")).thenReturn(r);
        RedisStandaloneResponse standaloneResponse = redisListEntry.send(pipeline, new NoRedisTtl());
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("LPUSH: 9, TTL: NoOp", standaloneResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForStandaloneWithTTL() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn(9L);
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(1L);
        when(pipeline.lpush("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisListEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("LPUSH: 9, TTL: UPDATED", standaloneResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForStandaloneWithTTLNotUpdated() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn(9L);
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(0L);
        when(pipeline.lpush("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisListEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("LPUSH: 9, TTL: NOT UPDATED", standaloneResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForStandalone() {
        Response r = Mockito.mock(Response.class);
        when(pipeline.lpush("test-key", "test-value")).thenReturn(r);
        when(r.get()).thenThrow(new JedisException("jedis error occurred"));
        RedisStandaloneResponse standaloneResponse = redisListEntry.send(pipeline, new NoRedisTtl());
        standaloneResponse.process();
        Assert.assertTrue(standaloneResponse.isFailed());
        Assert.assertEquals("jedis error occurred", standaloneResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionFromTTLForStandalone() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn(9L);
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenThrow(new JedisException("jedis error occurred"));
        when(pipeline.lpush("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisListEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertTrue(standaloneResponse.isFailed());
        Assert.assertEquals("jedis error occurred", standaloneResponse.getMessage());
    }
}
