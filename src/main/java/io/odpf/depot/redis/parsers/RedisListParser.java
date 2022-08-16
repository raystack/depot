package io.odpf.depot.redis.parsers;


import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisListEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis list parser.
 */
public class RedisListParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    public RedisListParser(OdpfMessageParser odpfMessageParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(odpfMessageParser, redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parseRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = parseKeyTemplate(redisSinkConfig.getSinkRedisKeyTemplate(), parsedOdpfMessage, schema);
        String field = redisSinkConfig.getSinkRedisListDataFieldName();
        if (field == null || field == "") {
            throw new IllegalArgumentException("Empty config SINK_REDIS_LIST_DATA_FIELD_NAME found");
        }
        String redisValue = parsedOdpfMessage.getFieldByName(field, schema).toString();
        if (redisValue == null) {
            throw new IllegalArgumentException("Invalid config SINK_REDIS_LIST_DATA_FIELD_NAME found");
        }
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        messageEntries.add(new RedisListEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisListEntry.class)));
        return messageEntries;
    }
}
