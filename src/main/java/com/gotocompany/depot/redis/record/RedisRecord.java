package com.gotocompany.depot.redis.record;

import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;


@AllArgsConstructor
public class RedisRecord {
    private RedisEntry redisEntry;
    @Getter
    private final Long index;
    @Getter
    private final ErrorInfo errorInfo;
    @Getter
    private final String metadata;
    @Getter
    private final boolean valid;

    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        return redisEntry.send(jedisPipelined, redisTTL);
    }

    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        return redisEntry.send(jedisCluster, redisTTL);
    }

    @Override
    public String toString() {
        return String.format("Metadata %s %s", metadata, redisEntry != null ? redisEntry.toString() : "NULL");
    }
}
