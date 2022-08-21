package io.odpf.depot.redis.client.response;

import org.junit.Assert;
import org.junit.Test;

public class RedisClusterResponseTest {
    private RedisClusterResponse redisClusterResponse;
    @Test
    public void shouldReportWhenSuccess() {
        redisClusterResponse = new RedisClusterResponse("Success", false);
        Assert.assertFalse(redisClusterResponse.isFailed());
        Assert.assertEquals("Success", redisClusterResponse.getMessage());
    }

    @Test
    public void shouldReportWhenFailed() {
        redisClusterResponse = new RedisClusterResponse("Failed", true);
        Assert.assertTrue(redisClusterResponse.isFailed());
        Assert.assertEquals("Failed", redisClusterResponse.getMessage());
    }
}
