package org.raystack.depot.redis.ttl;

import lombok.AllArgsConstructor;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@AllArgsConstructor
public class DurationTtl implements RedisTtl {
    private int ttlInSeconds;

    @Override
    public Response<Long> setTtl(Pipeline jedisPipelined, String key) {
        return jedisPipelined.expire(key, ttlInSeconds);
    }

    @Override
    public Long setTtl(JedisCluster jedisCluster, String key) {
        return jedisCluster.expire(key, ttlInSeconds);
    }
}
