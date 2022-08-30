package io.odpf.depot.redis.ttl;


import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.redis.enums.RedisSinkTtlType;

public class RedisTTLFactory {

    public static RedisTtl getTTl(RedisSinkConfig redisSinkConfig) {
        if (redisSinkConfig.getSinkRedisTtlType() == RedisSinkTtlType.DISABLE) {
            return new NoRedisTtl();
        }
        long redisTTLValue = redisSinkConfig.getSinkRedisTtlValue();
        if (redisTTLValue < 0) {
            throw new ConfigurationException("Provide a positive TTL value");
        }
        switch (redisSinkConfig.getSinkRedisTtlType()) {
            case EXACT_TIME:
                return new ExactTimeTtl(redisTTLValue);
            case DURATION:
                return new DurationTtl((int) redisTTLValue);
            default:
                throw new ConfigurationException("Not a valid TTL config");
        }
    }
}
