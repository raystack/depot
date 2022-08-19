package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
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
