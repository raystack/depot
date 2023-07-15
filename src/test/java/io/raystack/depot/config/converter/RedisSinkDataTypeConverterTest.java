package org.raystack.depot.config.converter;

import org.raystack.depot.redis.enums.RedisSinkDataType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSinkDataTypeConverterTest {

    private RedisSinkDataTypeConverter redisSinkDataTypeConverter;

    @Before
    public void setUp() {
        redisSinkDataTypeConverter = new RedisSinkDataTypeConverter();
    }

    @Test
    public void shouldReturnListSinkTypeFromLowerCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "list");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    @Test
    public void shouldReturnListSinkTypeFromUpperCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "LIST");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    @Test
    public void shouldReturnListSinkTypeFromMixedCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "LiSt");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    @Test
    public void shouldReturnHashSetSinkTypeFromInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "hashset");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.HASHSET));
    }

    @Test
    public void shouldReturnKeyValueSinkTypeFromInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "keyvalue");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.KEYVALUE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnEmptyArgument() {
        redisSinkDataTypeConverter.convert(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidArgument() {
        redisSinkDataTypeConverter.convert(null, "INVALID");
    }

}
