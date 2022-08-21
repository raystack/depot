package io.odpf.depot.redis.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisStandaloneResponse;
import io.odpf.depot.redis.entry.RedisEntry;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
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
    @Mock
    private Response response;

    @Test
    public void shouldSendUsingCLusterClient() {
        when(redisEntry.send(jedisCluster, redisTtl)).thenReturn(new RedisClusterResponse("OK", false));
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        RedisClusterResponse redisClusterResponse = redisRecord.send(jedisCluster, redisTtl);
        Assert.assertFalse(redisClusterResponse.isFailed());
        Assert.assertEquals("OK", redisClusterResponse.getMessage());
    }

    @Test
    public void shouldSendUsingStandaloneClient() {
        when(response.get()).thenReturn("Success response");
        RedisStandaloneResponse standaloneResponse = new RedisStandaloneResponse(response).process();
        when(redisEntry.send(jedisPipeline, redisTtl)).thenReturn(standaloneResponse);
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        RedisStandaloneResponse redisResponse = redisRecord.send(jedisPipeline, redisTtl);
        Assert.assertFalse(redisResponse.isFailed());
        Assert.assertEquals("Success response", redisResponse.getMessage());
    }

    @Test
    public void shouldGetToString() {
        when(redisEntry.toString()).thenReturn("RedisEntry REDIS ENTRY TO STRING");
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        Assert.assertEquals("Metadata METADATA\nRedisEntry REDIS ENTRY TO STRING", redisRecord.toString());
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
