package io.odpf.depot.config.converter;

import io.odpf.depot.redis.enums.RedisSinkDeploymentType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSinkDeploymentTypeConverterTest {
    private RedisSinkDeploymentTypeConverter redisSinkDeploymentTypeConverter;

    @Before
    public void setup() {
        redisSinkDeploymentTypeConverter = new RedisSinkDeploymentTypeConverter();
    }

    @Test
    public void shouldReturnStandaloneTypeFromLowerCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "standalone");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.STANDALONE));
    }

    @Test
    public void shouldReturnStandaloneTypeFromUpperCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "STANDALONE");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.STANDALONE));
    }

    @Test
    public void shouldReturnStandaloneTypeFromMixedCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "stANdAlOne");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.STANDALONE));
    }

    @Test
    public void shouldReturnClusterTypeFromUpperCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "CLUSTER");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.CLUSTER));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnEmptyArgument() {
        redisSinkDeploymentTypeConverter.convert(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidArgument() {
        redisSinkDeploymentTypeConverter.convert(null, "INVALID");
    }
}
