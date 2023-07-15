package org.raystack.depot.redis.record;

import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.redis.client.response.RedisClusterResponse;
import org.raystack.depot.redis.client.response.RedisStandaloneResponse;
import org.raystack.depot.redis.ttl.RedisTtl;
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
