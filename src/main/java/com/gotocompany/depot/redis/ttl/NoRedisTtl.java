package com.gotocompany.depot.redis.ttl;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class NoRedisTtl implements RedisTtl {
    @Override
    public Response<Long> setTtl(Pipeline jedisPipelined, String key) {
        return null;
    }

    @Override
    public Long setTtl(JedisCluster jedisCluster, String key) {
        return null;
    }
}
