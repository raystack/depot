package io.odpf.depot.redis.parsers;

import com.google.protobuf.DynamicMessage;
import io.odpf.depot.bigquery.converter.MessageRecordConverterUtils;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.message.*;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisKeyValueEntry;
import io.odpf.stencil.Parser;

import java.io.IOException;
import java.util.ArrayList;
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
            //TODO: use columns to build Redisdataentry
            List<RedisDataEntry> list = new ArrayList<>();
            return list;
        } catch (IOException e) {
            //log.error("failed to deserialize message: {}, {} ", e, message.getMetadataString());
            throw new DeserializerException("failed to deserialize ", e);
        }
    }
}
