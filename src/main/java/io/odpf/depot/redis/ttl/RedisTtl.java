package io.odpf.depot.redis.ttl;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * Interface for RedisTTL.
 */
public interface RedisTtl {
    Response<Long> setTtl(Pipeline jedisPipelined, String key);

    Long setTtl(JedisCluster jedisCluster, String key);
}
