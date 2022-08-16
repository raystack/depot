package io.odpf.depot.redis.parsers;


import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisHashSetFieldEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


/**
 * Redis hash set parser.
 */
public class RedisHashSetParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;
    public RedisHashSetParser(OdpfMessageParser odpfMessageParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(odpfMessageParser, redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parseRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = parseKeyTemplate(redisSinkConfig.getSinkRedisKeyTemplate(), parsedOdpfMessage, schema);
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        Properties properties = redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping();
        Set<String> keys =  properties.stringPropertyNames();
        for (String key : keys) {
            String value = properties.get(key).toString();
            String field = parseKeyTemplate(value, parsedOdpfMessage, schema);
            String redisValue = parsedOdpfMessage.getFieldByName(key, schema).toString();
            if (field == null) {
                throw new IllegalArgumentException("Empty or invalid config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found");
            }
            messageEntries.add(new RedisHashSetFieldEntry(redisKey, field, redisValue, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class)));
        }
        return messageEntries;
    }
}
