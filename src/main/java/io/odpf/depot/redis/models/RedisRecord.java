package io.odpf.depot.redis.models;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisStandaloneResponse;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;


@AllArgsConstructor
public class RedisRecord {
    private final RedisDataEntry redisDataEntry;
    @Getter
    private final Long index;
    @Getter
    private final ErrorInfo errorInfo;
    private final String metadata;
    @Getter
    private final boolean valid;

    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        return redisDataEntry.pushToRedis(jedisPipelined, redisTTL);
    }

    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        return redisDataEntry.pushToRedis(jedisCluster, redisTTL);
    }

    @Override
    public String toString() {
        return String.format("Metadata %s\n%s", metadata, redisDataEntry.toString());
    }
}
