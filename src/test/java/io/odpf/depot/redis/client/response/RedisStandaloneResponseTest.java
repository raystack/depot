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
    private RedisStandaloneResponse redisResponse;

    @Test
    public void shouldReportNotFailedWhenJedisExceptionNotThrown() {
        when(response.get()).thenReturn("Success response");
        redisResponse = new RedisStandaloneResponse(response);
        Assert.assertFalse(redisResponse.process().isFailed());
        Assert.assertEquals("Success response", redisResponse.process().getMessage());
    }

    @Test
    public void shouldReportFailedWhenJedisExceptionThrown() {
        when(response.get()).thenThrow(new JedisException("Failed response"));
        redisResponse = new RedisStandaloneResponse(response);
        Assert.assertTrue(redisResponse.process().isFailed());
        Assert.assertEquals("Failed response", redisResponse.process().getMessage());
    }
}
