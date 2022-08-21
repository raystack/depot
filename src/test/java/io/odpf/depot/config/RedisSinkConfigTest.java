package io.odpf.depot.config;

import io.odpf.depot.redis.enums.RedisSinkDeploymentType;
import io.odpf.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

public class RedisSinkConfigTest {
    @Test
    public void testMetadataTypes() {
        System.setProperty("SINK_REDIS_DEPLOYMENT_TYPE", "standalone");
        System.setProperty("SINK_REDIS_TTL_TYPE", "disable");
        System.setProperty("SINK_REDIS_KEY_TEMPLATE", "test-key");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, System.getProperties());
        Assert.assertEquals("test-key", config.getSinkRedisKeyTemplate());
        Assert.assertEquals(RedisSinkDeploymentType.STANDALONE, config.getSinkRedisDeploymentType());
        Assert.assertEquals(RedisSinkTtlType.DISABLE, config.getSinkRedisTtlType());
    }
}
