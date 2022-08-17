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
    private final RedisSinkConfig redisSinkConfig;
    private final StatsDReporter statsDReporter;

    public RedisKeyValueParser(OdpfMessageParser odpfMessageParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(odpfMessageParser, redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parseRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = parseKeyTemplate(redisSinkConfig.getSinkRedisKeyTemplate(), parsedOdpfMessage, schema);
        String fieldName = redisSinkConfig.getSinkRedisKeyValueDataFieldName();
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found");
        }
        String redisValue = parsedOdpfMessage.getFieldByName(fieldName, schema).toString();
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
