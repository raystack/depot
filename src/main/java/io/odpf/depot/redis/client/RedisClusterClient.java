package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.dataentry.RedisClusterResponse;
import io.odpf.depot.redis.dataentry.RedisResponse;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis cluster client.
 */
public class RedisClusterClient implements RedisClient {

    private final Instrumentation instrumentation;
    private final RedisTtl redisTTL;
    private final JedisCluster jedisCluster;

    public RedisClusterClient(Instrumentation instrumentation, RedisTtl redisTTL, JedisCluster jedisCluster) {
        this.instrumentation = instrumentation;
        this.redisTTL = redisTTL;
        this.jedisCluster = jedisCluster;
    }


    @Override
    public List<RedisResponse> execute(List<RedisRecord> records) {
        return records.stream()
                .map(record -> record.getRedisDataEntry().pushMessage(jedisCluster, redisTTL))
                .filter(RedisClusterResponse::isFailed)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedisCluster.close();
    }
}
