package org.raystack.depot.redis.parsers;

import org.raystack.depot.common.Template;
import org.raystack.depot.config.RedisSinkConfig;
import org.raystack.depot.exception.InvalidTemplateException;
import org.raystack.depot.message.OdpfMessageSchema;
import org.raystack.depot.metrics.StatsDReporter;

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
        Template keyTemplate;
        try {
            keyTemplate = new Template(redisSinkConfig.getSinkRedisKeyTemplate());
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
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
                        kv -> kv.getKey().toString(), kv -> {
                            try {
                                return new Template(kv.getValue().toString());
                            } catch (InvalidTemplateException e) {
                                throw new IllegalArgumentException(e.getMessage());
                            }
                        }));
                return new RedisHashSetEntryParser(statsDReporter, keyTemplate, fieldTemplates, schema);
        }
    }
}
