package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisKeyValueEntry;

import java.util.Collections;
import java.util.List;

public class RedisKeyValueParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    private OdpfMessageParser odpfMessageParser;

    public RedisKeyValueParser(OdpfMessageParser odpfMessageParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(odpfMessageParser, redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
        this.odpfMessageParser = odpfMessageParser;
    }

    @Override
    public List<RedisDataEntry> parseRedisEntry(ParsedOdpfMessage parsedOdpfMessage, String redisKey, OdpfMessageSchema schema) {
        String redisValue = parsedOdpfMessage.getFieldByName(redisSinkConfig.getSinkRedisKeyValueDataFieldName(), schema).toString();
        if (redisValue == null) {
            throw new IllegalArgumentException("Empty or invalid config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found");
        }
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
