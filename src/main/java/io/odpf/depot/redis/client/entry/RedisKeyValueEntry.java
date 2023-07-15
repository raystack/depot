package org.raystack.depot.redis.client.entry;

import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.redis.client.response.RedisClusterResponse;
import org.raystack.depot.redis.client.response.RedisStandaloneResponse;
import org.raystack.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

@AllArgsConstructor
@EqualsAndHashCode
public class RedisKeyValueEntry implements RedisEntry {
    private final String key;
    private final String value;
    @EqualsAndHashCode.Exclude
    private final Instrumentation instrumentation;

    @Override
    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        Response<String> response = jedisPipelined.set(key, value);
        Response<Long> ttlResponse = redisTTL.setTtl(jedisPipelined, key);
        return new RedisStandaloneResponse("SET", response, ttlResponse);
    }

    @Override
    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        try {
            String response = jedisCluster.set(key, value);
            Long ttlResponse = redisTTL.setTtl(jedisCluster, key);
            return new RedisClusterResponse("SET", response, ttlResponse);
        } catch (JedisException e) {
            return new RedisClusterResponse(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return String.format("RedisKeyValueEntry: Key %s, Value %s", key, value);
    }
}
