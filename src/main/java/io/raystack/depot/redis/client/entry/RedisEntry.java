package org.raystack.depot.redis.client.entry;

import org.raystack.depot.redis.client.response.RedisClusterResponse;
import org.raystack.depot.redis.client.response.RedisStandaloneResponse;
import org.raystack.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

/**
 * The interface Redis data entry.
 */
public interface RedisEntry {

    /**
     * Push messages to jedis pipeline.
     *
     * @param jedisPipelined the jedis pipelined
     * @param redisTTL       the redis ttl
     */
    RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL);

    /**
     * Push message to jedis cluster.
     *
     * @param jedisCluster the jedis cluster
     * @param redisTTL     the redis ttl
     */
    RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL);
}
