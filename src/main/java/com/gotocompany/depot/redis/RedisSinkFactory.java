package com.gotocompany.depot.redis;


import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.client.RedisClientFactory;
import com.gotocompany.depot.redis.parsers.RedisEntryParser;
import com.gotocompany.depot.redis.parsers.RedisEntryParserFactory;
import com.gotocompany.depot.redis.parsers.RedisParser;
import com.gotocompany.depot.utils.MessageConfigUtils;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;

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
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);
            Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
            MessageSchema schema = messageParser.getSchema(modeAndSchema.getSecond());
            RedisEntryParser redisEntryParser = RedisEntryParserFactory.getRedisEntryParser(sinkConfig, statsDReporter, schema);
            this.redisParser = new RedisParser(messageParser, redisEntryParser, modeAndSchema);
            instrumentation.logInfo("Connection to redis established successfully");
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception occurred while creating Redis sink", e);
        }
    }

    /**
     * We create redis client for each create call, because it's not thread safe.
     *
     * @return RedisSink
     */
    public Sink create() {
        return new RedisSink(
                RedisClientFactory.getClient(sinkConfig, statsDReporter),
                redisParser,
                new Instrumentation(statsDReporter, RedisSink.class));
    }
}
