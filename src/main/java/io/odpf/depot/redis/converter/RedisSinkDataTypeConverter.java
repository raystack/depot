package io.odpf.depot.redis.converter;

import io.odpf.depot.redis.enums.RedisSinkDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisSinkDataTypeConverter implements Converter<RedisSinkDataType> {
    @Override
    public RedisSinkDataType convert(Method method, String input) {
        return RedisSinkDataType.valueOf(input.toUpperCase());
    }
}
