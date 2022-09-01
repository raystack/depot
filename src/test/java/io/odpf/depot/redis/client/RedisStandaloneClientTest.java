package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.client.response.RedisStandaloneResponse;
import io.odpf.depot.redis.record.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;


@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneClientTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private RedisTtl redisTTL;
    @Mock
    private Jedis jedis;

    @Test
    public void shouldCloseTheClient() throws IOException {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        redisClient.close();

        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Closing Jedis client");
        Mockito.verify(jedis, Mockito.times(1)).close();
    }

    @Test
    public void shouldSendRecordsToJedis() {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, jedis);
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Response response = Mockito.mock(Response.class);
        Mockito.when(jedis.pipelined()).thenReturn(pipeline);
        Mockito.when(pipeline.exec()).thenReturn(response);
        Object ob = new Object();
        Mockito.when(response.get()).thenReturn(ob);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};
        List<RedisStandaloneResponse> responses = new ArrayList<RedisStandaloneResponse>() {{
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
        }};
        IntStream.range(0, redisRecords.size()).forEach(
                index -> {
                    Mockito.when(redisRecords.get(index).send(pipeline, redisTTL)).thenReturn(responses.get(index));
                    Mockito.when(responses.get(index).process()).thenReturn(responses.get(index));
                }
        );
        List<RedisResponse> actualResponses = redisClient.send(redisRecords);
        Mockito.verify(pipeline, Mockito.times(1)).multi();
        Mockito.verify(pipeline, Mockito.times(1)).sync();
        Mockito.verify(instrumentation, Mockito.times(1)).logDebug("jedis responses: {}", ob);
        IntStream.range(0, actualResponses.size()).forEach(
                index -> {
                    Assert.assertEquals(responses.get(index), actualResponses.get(index));
                }
        );
    }
}
