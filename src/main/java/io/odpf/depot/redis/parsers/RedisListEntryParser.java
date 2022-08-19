package io.odpf.depot.redis.parsers;


import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisListEntry;

import java.util.Collections;
import java.util.List;

/**
 * Redis list parser.
 */
public class RedisListEntryParser implements RedisEntryParser {
    private final RedisSinkConfig redisSinkConfig;
    private final StatsDReporter statsDReporter;

    public RedisListEntryParser(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = RedisParserUtils.parseTemplate(redisSinkConfig.getSinkRedisKeyTemplate(), parsedOdpfMessage, schema);
        String field = redisSinkConfig.getSinkRedisListDataFieldName();
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("Empty config SINK_REDIS_LIST_DATA_FIELD_NAME found");
        }
        String redisValue = parsedOdpfMessage.getFieldByName(field, schema).toString();
        if (redisValue == null) {
            throw new IllegalArgumentException("Invalid config SINK_REDIS_LIST_DATA_FIELD_NAME found");
        }
        return Collections.singletonList(new RedisListEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisListEntry.class)));
    }
}
