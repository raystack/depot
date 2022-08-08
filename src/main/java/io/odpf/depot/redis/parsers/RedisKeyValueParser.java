package io.odpf.depot.redis.parsers;

import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisKeyValueEntry;

import java.io.IOException;
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
    public List<RedisDataEntry> parse(OdpfMessage message) {
        try {
            SinkConnectorSchemaMessageMode mode = redisSinkConfig.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                    ? redisSinkConfig.getSinkConnectorSchemaProtoMessageClass() : redisSinkConfig.getSinkConnectorSchemaProtoKeyClass();
            ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
            String redisKey = parseKeyTemplate(redisSinkConfig.getSinkRedisKeyTemplate(), parsedOdpfMessage);
            String redisValue = parsedOdpfMessage.getFieldByName(redisSinkConfig.getRedisValueByName(), odpfMessageParser.getSchema(schemaClass));
            if (redisValue == null) {
                throw new IllegalArgumentException("Empty or invalid config REDIS_VALUE_BY_NAME found");
            }
            RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
            return Collections.singletonList(redisKeyValueEntry);
        } catch (IOException e) {
            throw new DeserializerException("failed to deserialize ", e);
        }
    }


}
