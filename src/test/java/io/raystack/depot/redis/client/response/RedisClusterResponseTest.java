package org.raystack.depot.redis.client.response;

import org.junit.Assert;
import org.junit.Test;

public class RedisClusterResponseTest {
    private RedisClusterResponse redisClusterResponse;

    @Test
    public void shouldReportWhenSuccess() {
        String response = "Success";
        Long ttlResponse = 1L;
        redisClusterResponse = new RedisClusterResponse("SET", response, ttlResponse);
        Assert.assertFalse(redisClusterResponse.isFailed());
        Assert.assertEquals("SET: Success, TTL: UPDATED", redisClusterResponse.getMessage());
    }

    @Test
    public void shouldReportWhenFailed() {
        redisClusterResponse = new RedisClusterResponse("Failed");
        Assert.assertTrue(redisClusterResponse.isFailed());
        Assert.assertEquals("Failed", redisClusterResponse.getMessage());
    }
}
