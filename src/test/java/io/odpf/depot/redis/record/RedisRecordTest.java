package io.odpf.depot.redis.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisStandaloneResponse;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisRecordTest {
    @Mock
    private RedisEntry redisEntry;
    @Mock
    private JedisCluster jedisCluster;
    @Mock
    private Pipeline jedisPipeline;
    @Mock
    private RedisTtl redisTtl;

    @Test
    public void shouldSendUsingCLusterClient() {
        RedisClusterResponse response = Mockito.mock(RedisClusterResponse.class);
        when(redisEntry.send(jedisCluster, redisTtl)).thenReturn(response);
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        RedisClusterResponse redisClusterResponse = redisRecord.send(jedisCluster, redisTtl);
        Assert.assertEquals(response, redisClusterResponse);
    }

    @Test
    public void shouldSendUsingStandaloneClient() {
        RedisStandaloneResponse standaloneResponse = Mockito.mock(RedisStandaloneResponse.class);
        when(redisEntry.send(jedisPipeline, redisTtl)).thenReturn(standaloneResponse);
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        RedisStandaloneResponse redisResponse = redisRecord.send(jedisPipeline, redisTtl);
        Assert.assertEquals(standaloneResponse, redisResponse);
    }

    @Test
    public void shouldGetToString() {
        when(redisEntry.toString()).thenReturn("RedisEntry REDIS ENTRY TO STRING");
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        Assert.assertEquals("Metadata METADATA RedisEntry REDIS ENTRY TO STRING", redisRecord.toString());
    }

    @Test
    public void shouldGetRecordIndex() {
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        Assert.assertEquals(new Long(0), redisRecord.getIndex());
    }

    @Test
    public void shouldGetRecordErrorInfo() {
        ErrorInfo errorInfo = new ErrorInfo(new Exception(""), ErrorType.DEFAULT_ERROR);
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, errorInfo, "METADATA", true);
        Assert.assertEquals(errorInfo, redisRecord.getErrorInfo());
    }

    @Test
    public void shouldGetRecordValidBoolean() {
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        Assert.assertTrue(redisRecord.isValid());
    }
}
