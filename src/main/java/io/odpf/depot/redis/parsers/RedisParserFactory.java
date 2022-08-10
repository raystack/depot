package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.metrics.StatsDReporter;

/**
 * Redis parser factory.
 */
public class RedisParserFactory {

    /**
     * Gets parser.
     *
     * @param redisSinkConfig    the redis sink config
     * @param statsDReporter     the statsd reporter
     * @return RedisParser
     */
    public static RedisParser getParser(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        switch (redisSinkConfig.getSinkRedisDataType()) {
            case KEYVALUE:
                return new RedisKeyValueParser(OdpfMessageParserFactory.getParser(redisSinkConfig, statsDReporter), redisSinkConfig, statsDReporter);
            case LIST:
                return new RedisListParser(OdpfMessageParserFactory.getParser(redisSinkConfig, statsDReporter), redisSinkConfig, statsDReporter);
            case HASHSET:
                return new RedisHashSetParser(OdpfMessageParserFactory.getParser(redisSinkConfig, statsDReporter), redisSinkConfig, statsDReporter);
            default:
                return null;
        }
    }
}
