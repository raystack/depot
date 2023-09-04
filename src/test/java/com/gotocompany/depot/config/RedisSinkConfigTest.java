package com.gotocompany.depot.config;

import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

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

    @Test
    public void shouldSetNullIfAuthConfigsSetAsEmptyString() {
        Properties properties = new Properties();
        properties.setProperty("SINK_REDIS_AUTH_USERNAME", "");
        properties.setProperty("SINK_REDIS_AUTH_PASSWORD", "");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertNull(config.getSinkRedisAuthUsername());
        Assert.assertNull(config.getSinkRedisAuthPassword());
    }
    @Test
    public void shouldReturnNullIfAuthConfigsNotSet() {
        Properties properties = new Properties();
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertNull(config.getSinkRedisAuthUsername());
        Assert.assertNull(config.getSinkRedisAuthPassword());
    }

    @Test
    public void shouldReturnConfigsIfAuthConfigsNotEmpty() {
        Properties properties = new Properties();
        properties.setProperty("SINK_REDIS_AUTH_USERNAME", "user");
        properties.setProperty("SINK_REDIS_AUTH_PASSWORD", "pwd");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertEquals("user", config.getSinkRedisAuthUsername());
        Assert.assertEquals("pwd", config.getSinkRedisAuthPassword());
    }

    @Test
    public void shouldRemoveWhiteSpacesFromConfigs() {
        Properties properties = new Properties();
        properties.setProperty("SINK_REDIS_URLS", "     0.0.0.0:8000      ");
        properties.setProperty("SINK_REDIS_AUTH_USERNAME", " user ");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertEquals("0.0.0.0:8000", config.getSinkRedisUrls());
        Assert.assertEquals("user", config.getSinkRedisAuthUsername());
        Assert.assertNull(config.getSinkRedisAuthPassword());
    }
}
