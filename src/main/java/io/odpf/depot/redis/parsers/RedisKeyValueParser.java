package io.odpf.depot.redis.parsers;

import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisKeyValueEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    public List<RedisDataEntry> parse(OdpfMessage message) {
        try {
            SinkConnectorSchemaMessageMode mode = redisSinkConfig.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                    ? redisSinkConfig.getSinkConnectorSchemaProtoMessageClass() : redisSinkConfig.getSinkConnectorSchemaProtoKeyClass();
            ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
            OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass);
            Map<String, Object> columns = parsedOdpfMessage.getMapping(schema);
            // use columns to build Key and Redisdataentry
            String redisKey = redisSinkConfig.getSinkRedisKeyTemplate();
            String redisValue = (String) columns.get(redisSinkConfig.getRedisValueByName());
            if (redisValue == null) {
                throw new IllegalArgumentException("Please provide REDIS_VALUE_BY_NAME in key value sink");
            }
            RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
            return Collections.singletonList(redisKeyValueEntry);
        } catch (IOException e) {
            //log.error("failed to deserialize message: {}, {} ", e, message.getMetadataString());
            throw new DeserializerException("failed to deserialize ", e);
        }
    }
}
