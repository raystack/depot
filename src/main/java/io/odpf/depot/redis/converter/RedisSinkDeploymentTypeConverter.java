package io.odpf.depot.redis.converter;

import io.odpf.depot.redis.enums.RedisSinkDeploymentType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisSinkDeploymentTypeConverter implements Converter<RedisSinkDeploymentType> {
    @Override
    public RedisSinkDeploymentType convert(Method method, String input) {
        return RedisSinkDeploymentType.valueOf(input.toUpperCase());
    }
}
