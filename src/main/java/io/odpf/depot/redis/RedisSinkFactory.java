package io.odpf.depot.redis;


import com.timgroup.statsd.NoOpStatsDClient;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.RedisClientFactory;
import io.odpf.depot.redis.parsers.RedisEntryParser;
import io.odpf.depot.redis.parsers.RedisEntryParserFactory;
import io.odpf.depot.redis.parsers.RedisParser;
import io.odpf.depot.utils.MessageConfigUtils;

import java.io.IOException;

public class RedisSinkFactory {
    private final RedisSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private RedisParser redisParser;

    public RedisSinkFactory(RedisSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public RedisSinkFactory(RedisSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = new StatsDReporter(new NoOpStatsDClient());
    }

    public void init() {
        try {
            Instrumentation instrumentation = new Instrumentation(statsDReporter, RedisSinkFactory.class);
            String redisConfig = String.format("\n\tredis.urls = %s\n\tredis.key.template = %s\n\tredis.sink.data.type = %s"
                            + "\n\tredis.deployment.type = %s\n\tredis.ttl.type = %s\n\tredis.ttl.value = %d\n\t",
                    sinkConfig.getSinkRedisUrls(),
                    sinkConfig.getSinkRedisKeyTemplate(),
                    sinkConfig.getSinkRedisDataType().toString(),
                    sinkConfig.getSinkRedisDeploymentType().toString(),
                    sinkConfig.getSinkRedisTtlType().toString(),
                    sinkConfig.getSinkRedisTtlValue());
            switch (sinkConfig.getSinkRedisDataType()) {
                case LIST:
                    redisConfig += "redis.list.data.field.name=" + sinkConfig.getSinkRedisListDataFieldName();
                    break;
                case KEYVALUE:
                    redisConfig += "redis.keyvalue.data.field.name=" + sinkConfig.getSinkRedisKeyValueDataFieldName();
                    break;
                case HASHSET:
                    redisConfig += "redis.hashset.field.to.column.mapping=" + sinkConfig.getSinkRedisHashsetFieldToColumnMapping().toString();
                    break;
                default:
            }
            instrumentation.logInfo(redisConfig);
            instrumentation.logInfo("Redis server type = {}", sinkConfig.getSinkRedisDeploymentType());
            OdpfMessageParser messageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
            Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
            OdpfMessageSchema schema = messageParser.getSchema(modeAndSchema.getSecond());
            RedisEntryParser redisEntryParser = RedisEntryParserFactory.getRedisEntryParser(sinkConfig, statsDReporter, schema);
            this.redisParser = new RedisParser(messageParser, redisEntryParser, modeAndSchema);
            instrumentation.logInfo("Connection to redis established successfully");
        } catch (IOException | InvalidTemplateException e) {
            throw new IllegalArgumentException("Exception occurred while creating Redis sink", e);
        }
    }

    /**
     * We create redis client for each create call, because it's not thread safe.
     *
     * @return RedisSink
     */
    public OdpfSink create() {
        return new RedisSink(
                RedisClientFactory.getClient(sinkConfig, statsDReporter),
                redisParser,
                new Instrumentation(statsDReporter, RedisSink.class));
    }
}
