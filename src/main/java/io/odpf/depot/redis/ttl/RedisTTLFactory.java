package io.odpf.depot.redis.ttl;


import io.odpf.depot.config.RedisSinkConfig;

public class RedisTTLFactory {

    public static RedisTtl getTTl(RedisSinkConfig redisSinkConfig) {
        long redisTTLValue = redisSinkConfig.getSinkRedisTtlValue();
        if (redisTTLValue < 0) {
            //throw new ConfigurationException("Provide a positive TTL value");
        }
        switch (redisSinkConfig.getSinkRedisTtlType()) {
            case EXACT_TIME:
                return new ExactTimeTtl(redisTTLValue);
            case DURATION:
                return new DurationTtl((int) redisTTLValue);
            default:
                return new NoRedisTtl();
        }
    }
}
