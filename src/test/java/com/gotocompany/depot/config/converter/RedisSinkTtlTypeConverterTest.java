package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSinkTtlTypeConverterTest {
    private RedisSinkTtlTypeConverter redisSinkTtlTypeConverter;

    @Before
    public void setUp() {
        redisSinkTtlTypeConverter = new RedisSinkTtlTypeConverter();
    }

    @Test
    public void shouldReturnExactTimeTypeFromLowerCaseInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "exact_time");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.EXACT_TIME));
    }

    @Test
    public void shouldReturnExactTimeTypeFromUpperCaseInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "EXACT_TIME");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.EXACT_TIME));
    }

    @Test
    public void shouldReturnExactTimeTypeFromMixedCaseInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "eXAct_TiMe");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.EXACT_TIME));
    }

    @Test
    public void shouldReturnDisableTypeFromInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "DISABLE");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.DISABLE));
    }

    @Test
    public void shouldReturnDurationTypeFromInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "DURATION");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.DURATION));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnEmptyArgument() {
        redisSinkTtlTypeConverter.convert(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidArgument() {
        redisSinkTtlTypeConverter.convert(null, "INVALID");
    }
}
