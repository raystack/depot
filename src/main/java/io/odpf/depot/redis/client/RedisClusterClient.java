package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Response;

import java.util.List;

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
    public Response execute(List<RedisRecord> records) {
        records.forEach(record -> record.getRedisDataEntry().pushMessage(jedisCluster, redisTTL));
        return null;
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedisCluster.close();
    }
}
