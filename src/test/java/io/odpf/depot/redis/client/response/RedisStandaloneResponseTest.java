package io.odpf.depot.redis.client.response;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneResponseTest {
    @Mock
    private Response response;
    @Mock
    private Response ttlResponse;
    private RedisStandaloneResponse redisResponse;

    @Test
    public void shouldReportNotFailedWhenJedisExceptionNotThrown() {
        when(response.get()).thenReturn("Success response");
        when(ttlResponse.get()).thenReturn(1L);
        redisResponse = new RedisStandaloneResponse("SET", response, ttlResponse);
        Assert.assertFalse(redisResponse.process().isFailed());
        Assert.assertEquals("SET: Success response, TTL: UPDATED", redisResponse.process().getMessage());
    }

    @Test
    public void shouldReportFailedWhenJedisExceptionThrown() {
        when(response.get()).thenThrow(new JedisException("Failed response"));
        redisResponse = new RedisStandaloneResponse("SET", response, ttlResponse);
        Assert.assertTrue(redisResponse.process().isFailed());
        Assert.assertEquals("Failed response", redisResponse.process().getMessage());
    }
}
