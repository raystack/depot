package io.odpf.depot.redis.dataentry;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@Getter
public class RedisHashSetFieldEntry implements RedisDataEntry {

    private String key;
    private String field;
    private String value;
    private Instrumentation instrumentation;

    @Override
    public void pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL) {
        getInstrumentation().logDebug("key: {}, field: {}, value: {}", getKey(), getField(), getValue());
        jedisPipelined.hset(getKey(), getField(), getValue());
        redisTTL.setTtl(jedisPipelined, getKey());
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL) {
        getInstrumentation().logDebug("key: {}, field: {}, value: {}", getKey(), getField(), getValue());
        jedisCluster.hset(getKey(), getField(), getValue());
        redisTTL.setTtl(jedisCluster, getKey());
    }
}
