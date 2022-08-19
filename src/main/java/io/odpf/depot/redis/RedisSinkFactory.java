package io.odpf.depot.redis;


import io.odpf.depot.OdpfSink;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.RedisClient;
import io.odpf.depot.redis.client.RedisClientFactory;
import io.odpf.depot.redis.parsers.RedisEntryParser;
import io.odpf.depot.redis.parsers.RedisEntryParserFactory;
import io.odpf.depot.redis.parsers.RedisParser;

public class RedisSinkFactory {

    private final RedisSinkConfig sinkConfig;

    private final StatsDReporter statsDReporter;
    private RedisParser redisParser;
    private RedisClient redisClient;


    public RedisSinkFactory(RedisSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public void init() {
        Instrumentation instrumentation = new Instrumentation(statsDReporter, RedisSinkFactory.class);
        String redisConfig = String.format("\n\tredis.urls = %s\n\tredis.key.template = %s\n\tredis.sink.type = %s"
                        + "\n\tredis.list.data.proto.index = %s\n\tredis.ttl.type = %s\n\tredis.ttl.value = %d",
                sinkConfig.getSinkRedisUrls(),
                sinkConfig.getSinkRedisKeyTemplate(),
                sinkConfig.getSinkRedisDataType().toString(),
                sinkConfig.getSinkRedisListDataProtoIndex(),
                sinkConfig.getSinkRedisTtlType().toString(),
                sinkConfig.getSinkRedisTtlValue());
        instrumentation.logDebug(redisConfig);
        instrumentation.logInfo("Redis server type = {}", sinkConfig.getSinkRedisDeploymentType());

        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, sinkConfig);
        this.redisClient = redisClientFactory.getClient();
        OdpfMessageParser messageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
        RedisEntryParser redisEntryParser = RedisEntryParserFactory.getRedisEntryParser(sinkConfig, statsDReporter);
        this.redisParser = new RedisParser(sinkConfig, messageParser, redisEntryParser);
        instrumentation.logInfo("Connection to redis established successfully");
    }

    public OdpfSink create() {
        return new RedisSink(redisClient, redisParser, new Instrumentation(statsDReporter, RedisSink.class));
    }
}
