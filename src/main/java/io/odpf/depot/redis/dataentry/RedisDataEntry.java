package io.odpf.depot.redis.dataentry;

import io.odpf.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

/**
 * The interface Redis data entry.
 */
public interface RedisDataEntry {

    /**
     * Push messages to jedis pipeline.
     *
     * @param jedisPipelined the jedis pipelined
     * @param redisTTL       the redis ttl
     */
    RedisStandaloneResponse pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL);

    /**
     * Push message to jedis cluster.
     *
     * @param jedisCluster the jedis cluster
     * @param redisTTL     the redis ttl
     */
    RedisClusterResponse pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL);
}
