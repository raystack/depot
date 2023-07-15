package org.raystack.depot.redis.client;

import org.raystack.depot.config.RedisSinkConfig;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.redis.enums.RedisSinkDeploymentType;
import org.raystack.depot.redis.enums.RedisSinkTtlType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisClientFactoryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StatsDReporter statsDReporter;

    @Test
    public void shouldGetStandaloneClient() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.STANDALONE);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("0.0.0.0:0");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);

        Assert.assertEquals(RedisStandaloneClient.class, client.getClass());
    }

    @Test
    public void shouldGetStandaloneClientWhenURLHasSpaces() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.STANDALONE);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn(" 0.0.0.0:0 ");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);

        Assert.assertEquals(RedisStandaloneClient.class, client.getClass());
    }

    @Test
    public void shouldGetClusterClient() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("0.0.0.0:0, 1.1.1.1:1");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    @Test
    public void shouldGetClusterClientWhenURLHasSpaces() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn(" 0.0.0.0:0, 1.1.1.1:1 ");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForCluster() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Invalid url(s) for redis cluster: localhost:6379,localhost:6378,localhost");

        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("localhost:6379,localhost:6378,localhost");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);
    }

    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForStandalone() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Invalid url for redis standalone: localhost");

        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.STANDALONE);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("localhost");

        RedisClientFactory.getClient(redisSinkConfig, statsDReporter);
    }
}
