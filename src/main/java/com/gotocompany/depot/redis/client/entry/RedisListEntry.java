package com.gotocompany.depot.redis.client.entry;

import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.ttl.RedisTtl;
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
public class RedisListEntry implements RedisEntry {
    private final String key;
    private final String value;
    @EqualsAndHashCode.Exclude
    private final Instrumentation instrumentation;

    @Override
    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        Response<Long> response = jedisPipelined.lpush(key, value);
        Response<Long> ttlResponse = redisTTL.setTtl(jedisPipelined, key);
        return new RedisStandaloneResponse("LPUSH", response, ttlResponse);
    }

    @Override
    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        try {
            Long response = jedisCluster.lpush(key, value);
            Long ttlResponse = redisTTL.setTtl(jedisCluster, key);
            return new RedisClusterResponse("LPUSH", response, ttlResponse);
        } catch (JedisException e) {
            return new RedisClusterResponse(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return String.format("RedisListEntry: Key %s, Value %s", key, value);
    }
}
