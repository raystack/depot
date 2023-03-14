package com.gotocompany.depot.redis.ttl;

import lombok.AllArgsConstructor;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;


@AllArgsConstructor
public class ExactTimeTtl implements RedisTtl {
    private long unixTime;

    @Override
    public Response<Long> setTtl(Pipeline jedisPipelined, String key) {
        return jedisPipelined.expireAt(key, unixTime);
    }

    @Override
    public Long setTtl(JedisCluster jedisCluster, String key) {
        return jedisCluster.expireAt(key, unixTime);
    }
}
