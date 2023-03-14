package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@RunWith(MockitoJUnitRunner.class)
public class RedisClusterClientTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private RedisTtl redisTTL;
    @Mock
    private JedisCluster jedisCluster;

    @Test
    public void shouldSendToRedisCluster() {
        RedisClient redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};
        List<RedisClusterResponse> responses = new ArrayList<RedisClusterResponse>() {{
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
        }};
        IntStream.range(0, redisRecords.size()).forEach(
                index -> Mockito.when(redisRecords.get(index).send(jedisCluster, redisTTL)).thenReturn(responses.get(index))
        );
        List<RedisResponse> actualResponse = redisClient.send(redisRecords);
        IntStream.range(0, redisRecords.size()).forEach(
                index -> Assert.assertEquals(responses.get(index), actualResponse.get(index)));
    }

    @Test
    public void shouldCallClose() throws IOException {
        RedisClient redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        redisClient.close();
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Closing Jedis client");
        Mockito.verify(jedisCluster, Mockito.times(1)).close();
    }
}
