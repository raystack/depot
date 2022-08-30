package io.odpf.depot.redis.client.entry;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisStandaloneResponse;
import io.odpf.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@EqualsAndHashCode
public class RedisHashSetFieldEntry implements RedisEntry {

    private final String key;
    private final String field;
    private final String value;
    @EqualsAndHashCode.Exclude
    private final Instrumentation instrumentation;

    @Override
    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, field: {}, value: {}", key, field, value);
        Response<Long> response = jedisPipelined.hset(key, field, value);
        Response<Long> ttlResponse = redisTTL.setTtl(jedisPipelined, key);
        return new RedisStandaloneResponse("HSET", response, ttlResponse);
    }

    @Override
    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, field: {}, value: {}", key, field, value);
        try {
            Long response = jedisCluster.hset(key, field, value);
            Long ttlResponse = redisTTL.setTtl(jedisCluster, key);
            return new RedisClusterResponse("HSET", response, ttlResponse);
        } catch (JedisException e) {
            return new RedisClusterResponse(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return String.format("RedisHashSetFieldEntry Key %s, Field %s, Value %s", key, field, value);
    }
}
