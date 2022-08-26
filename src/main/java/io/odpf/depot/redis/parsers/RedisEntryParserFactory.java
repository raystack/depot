package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.metrics.StatsDReporter;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Redis parser factory.
 */
public class RedisEntryParserFactory {

    public static RedisEntryParser getRedisEntryParser(
            RedisSinkConfig redisSinkConfig,
            StatsDReporter statsDReporter,
            OdpfMessageSchema schema) {
        Template keyTemplate = new Template(redisSinkConfig.getSinkRedisKeyTemplate());
        switch (redisSinkConfig.getSinkRedisDataType()) {
            case KEYVALUE:
                String fieldName = redisSinkConfig.getSinkRedisKeyValueDataFieldName();
                if (fieldName == null || fieldName.isEmpty()) {
                    throw new IllegalArgumentException("Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found");
                }
                return new RedisKeyValueEntryParser(statsDReporter, keyTemplate, fieldName, schema);
            case LIST:
                String field = redisSinkConfig.getSinkRedisListDataFieldName();
                if (field == null || field.isEmpty()) {
                    throw new IllegalArgumentException("Empty config SINK_REDIS_LIST_DATA_FIELD_NAME found");
                }
                return new RedisListEntryParser(statsDReporter, keyTemplate, field, schema);
            default:
                Properties properties = redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping();
                if (properties == null || properties.isEmpty()) {
                    throw new IllegalArgumentException("Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found");
                }
                Map<String, Template> fieldTemplates = properties.entrySet().stream().collect(Collectors.toMap(
                        kv -> kv.getKey().toString(), kv -> new Template(kv.getValue().toString())
                ));
                return new RedisHashSetEntryParser(statsDReporter, keyTemplate, fieldTemplates, schema);
        }
    }
}
