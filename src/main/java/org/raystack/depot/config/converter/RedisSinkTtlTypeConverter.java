package org.raystack.depot.config.converter;

import org.raystack.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisSinkTtlTypeConverter implements Converter<RedisSinkTtlType> {
    @Override
    public RedisSinkTtlType convert(Method method, String input) {
        return RedisSinkTtlType.valueOf(input.toUpperCase());
    }
}
