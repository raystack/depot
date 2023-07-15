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
public class RedisHashSetFieldEntryTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Pipeline pipeline;
    @Mock
    private JedisCluster jedisCluster;
    private RedisHashSetFieldEntry redisHashSetFieldEntry;

    @Before
    public void setup() {
        redisHashSetFieldEntry = new RedisHashSetFieldEntry("test-key", "test-field", "test-value", instrumentation);
    }

    @Test
    public void shouldSentToRedisForCluster() {
        when(jedisCluster.hset("test-key", "test-field", "test-value")).thenReturn(9L);
        RedisClusterResponse clusterResponse = redisHashSetFieldEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
        Assert.assertEquals("HSET: 9, TTL: NoOp", clusterResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForClusterWithTTL() {
        when(jedisCluster.hset("test-key", "test-field", "test-value")).thenReturn(9L);
        when(jedisCluster.expire("test-key", 1000)).thenReturn(1L);
        RedisClusterResponse clusterResponse = redisHashSetFieldEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
        Assert.assertEquals("HSET: 9, TTL: UPDATED", clusterResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForClusterWithTTLNotUpdated() {
        when(jedisCluster.hset("test-key", "test-field", "test-value")).thenReturn(9L);
        when(jedisCluster.expire("test-key", 1000)).thenReturn(0L);
        RedisClusterResponse clusterResponse = redisHashSetFieldEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
        Assert.assertEquals("HSET: 9, TTL: NOT UPDATED", clusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.hset("test-key", "test-field", "test-value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisHashSetFieldEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionFromTTLForCluster() {
        when(jedisCluster.hset("test-key", "test-field", "test-value")).thenReturn(10L);
        when(jedisCluster.expire("test-key", 1000)).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisHashSetFieldEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    @Test
    public void shouldGetSetEntryToString() {
        String expected = "RedisHashSetFieldEntry Key test-key, Field test-field, Value test-value";
        Assert.assertEquals(expected, redisHashSetFieldEntry.toString());
    }

    @Test
    public void shouldSentToRedisForStandAlone() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn(9L);
        when(pipeline.hset("test-key", "test-field", "test-value")).thenReturn(r);
        RedisStandaloneResponse standaloneResponse = redisHashSetFieldEntry.send(pipeline, new NoRedisTtl());
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field",
                "test-value");
        Assert.assertEquals("HSET: 9, TTL: NoOp", standaloneResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForStandaloneWithTTL() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn(9L);
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(1L);
        when(pipeline.hset("test-key", "test-field", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisHashSetFieldEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field",
                "test-value");
        Assert.assertEquals("HSET: 9, TTL: UPDATED", standaloneResponse.getMessage());
    }

    @Test
    public void shouldSentToRedisForStandaloneWithTTLNotUpdated() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn(9L);
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(0L);
        when(pipeline.hset("test-key", "test-field", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisHashSetFieldEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field",
                "test-value");
        Assert.assertEquals("HSET: 9, TTL: NOT UPDATED", standaloneResponse.getMessage());
    }

    @Test
    public void shouldReportFailedForJedisExceptionForStandalone() {
        Response r = Mockito.mock(Response.class);
        when(pipeline.hset("test-key", "test-field", "test-value")).thenReturn(r);
        when(r.get()).thenThrow(new JedisException("jedis error occurred"));
        RedisStandaloneResponse standaloneResponse = redisHashSetFieldEntry.send(pipeline, new NoRedisTtl());
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
        when(pipeline.hset("test-key", "test-field", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisHashSetFieldEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertTrue(standaloneResponse.isFailed());
        Assert.assertEquals("jedis error occurred", standaloneResponse.getMessage());
    }
}
