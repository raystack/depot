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
        when(response.get()).thenReturn("");
        redisResponse = new RedisStandaloneResponse(response);
        Assert.assertFalse(redisResponse.process().isFailed());
    }

    @Test
    public void shouldReportFailedWhenJedisExceptionThrown() {
        when(response.get()).thenThrow(new JedisException(""));
        redisResponse = new RedisStandaloneResponse(response);
        Assert.assertTrue(redisResponse.process().isFailed());
    }

    @Test
    public void shouldSetResponseMessageWhenJedisExceptionNotThrown() {
        when(response.get()).thenReturn("Success reponse");
        redisResponse = new RedisStandaloneResponse(response);
        Assert.assertEquals("Success reponse", redisResponse.process().getMessage());
    }

    @Test
    public void shouldSetResponseMessageWhenJedisExceptionThrown() {
        when(response.get()).thenReturn("Failed reponse");
        redisResponse = new RedisStandaloneResponse(response);
        Assert.assertEquals("Failed reponse", redisResponse.process().getMessage());
    }
}
