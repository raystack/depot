package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.entry.RedisEntry;
import io.odpf.depot.redis.entry.RedisKeyValueEntry;

import java.util.Collections;
import java.util.List;

public class RedisKeyValueEntryParser implements RedisEntryParser {
    private final RedisSinkConfig redisSinkConfig;
    private final StatsDReporter statsDReporter;

    public RedisKeyValueEntryParser(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = RedisParserUtils.parseTemplate(redisSinkConfig.getSinkRedisKeyTemplate(), parsedOdpfMessage, schema);
        String fieldName = redisSinkConfig.getSinkRedisKeyValueDataFieldName();
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found");
        }
        String redisValue = parsedOdpfMessage.getFieldByName(fieldName, schema).toString();
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, io.odpf.depot.redis.entry.RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
