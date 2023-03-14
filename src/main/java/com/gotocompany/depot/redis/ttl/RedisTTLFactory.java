package com.gotocompany.depot.redis.ttl;


import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;

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
