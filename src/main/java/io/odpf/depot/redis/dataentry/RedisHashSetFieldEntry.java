package io.odpf.depot.redis.dataentry;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@EqualsAndHashCode
public class RedisHashSetFieldEntry implements RedisDataEntry {

    private final String key;
    private final String field;
    private final String value;
    @EqualsAndHashCode.Exclude
    private final Instrumentation instrumentation;

    @Override
    public void pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, field: {}, value: {}", key, field, value);
        jedisPipelined.hset(key, field, value);
        redisTTL.setTtl(jedisPipelined, key);
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, field: {}, value: {}", key, field, value);
        jedisCluster.hset(key, field, value);
        redisTTL.setTtl(jedisCluster, key);
    }
}
