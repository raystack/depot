package org.raystack.depot.redis.client.entry;

import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.redis.client.response.RedisClusterResponse;
import org.raystack.depot.redis.client.response.RedisStandaloneResponse;
import org.raystack.depot.redis.ttl.DurationTtl;
import org.raystack.depot.redis.ttl.NoRedisTtl;
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
public class RedisKeyValueEntryTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Pipeline pipeline;
    @Mock
    private JedisCluster jedisCluster;
    private RedisKeyValueEntry redisKeyValueEntry;

    @Before
    public void setup() {
        redisKeyValueEntry = new RedisKeyValueEntry("test-key", "test-value", instrumentation);
    }

    @Test
    public void shouldSentToRedisForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NoOp", clusterResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForClusterWithTTL() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        when(jedisCluster.expire("test-key", 1000)).thenReturn(1L);
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: UPDATED", clusterResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForClusterWithTTLNotUpdated() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        when(jedisCluster.expire("test-key", 1000)).thenReturn(0L);
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NOT UPDATED", clusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionFromTTLForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        when(jedisCluster.expire("test-key", 1000)).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    @Test
    public void shouldGetEntryToString() {
        String expected = "RedisKeyValueEntry: Key test-key, Value test-value";
        Assert.assertEquals(expected, redisKeyValueEntry.toString());
    }

    @Test
    public void shouldSentToRedisForStandAlone() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new NoRedisTtl());
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NoOp", standaloneResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForStandaloneWithTTL() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(1L);
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: UPDATED", standaloneResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForStandaloneWithTTLNotUpdated() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(0L);
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NOT UPDATED", standaloneResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForStandalone() {
        Response r = Mockito.mock(Response.class);
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(r.get()).thenThrow(new JedisException("jedis error occurred"));
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new NoRedisTtl());
        standaloneResponse.process();
        Assert.assertTrue(standaloneResponse.isFailed());
        Assert.assertEquals("jedis error occurred", standaloneResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionFromTTLForStandalone() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenThrow(new JedisException("jedis error occurred"));
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertTrue(standaloneResponse.isFailed());
        Assert.assertEquals("jedis error occurred", standaloneResponse.getMessage());
    }
}
