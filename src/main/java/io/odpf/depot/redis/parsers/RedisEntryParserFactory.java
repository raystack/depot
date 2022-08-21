package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.metrics.StatsDReporter;

/**
 * Redis parser factory.
 */
public class RedisEntryParserFactory {

    public static RedisEntryParser getRedisEntryParser(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        switch (redisSinkConfig.getSinkRedisDataType()) {
            case KEYVALUE:
                return new RedisKeyValueEntryParser(redisSinkConfig, statsDReporter);
            case LIST:
                return new RedisListEntryParser(redisSinkConfig, statsDReporter);
            default:
                return new RedisHashSetEntryParser(redisSinkConfig, statsDReporter);
        }
    }
}
