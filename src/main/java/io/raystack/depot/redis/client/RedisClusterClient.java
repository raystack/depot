package org.raystack.depot.redis.client;

import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.redis.client.response.RedisResponse;
import org.raystack.depot.redis.record.RedisRecord;
import org.raystack.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis cluster client.
 */
@AllArgsConstructor
public class RedisClusterClient implements RedisClient {

    private final Instrumentation instrumentation;
    private final RedisTtl redisTTL;
    private final JedisCluster jedisCluster;

    @Override
    public List<RedisResponse> send(List<RedisRecord> records) {
        return records.stream()
                .map(record -> record.send(jedisCluster, redisTTL))
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedisCluster.close();
    }
}
