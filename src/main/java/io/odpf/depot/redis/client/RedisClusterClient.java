package io.odpf.depot.redis.client;

import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis cluster client.
 */
public class RedisClusterClient implements RedisClient {

    private Instrumentation instrumentation;
    private RedisTtl redisTTL;
    private JedisCluster jedisCluster;
    public RedisClusterClient(Instrumentation instrumentation, RedisTtl redisTTL, JedisCluster jedisCluster) {
        this.instrumentation = instrumentation;
        this.redisTTL = redisTTL;
        this.jedisCluster = jedisCluster;
    }


    @Override
    public List<OdpfMessage> execute(List<RedisRecord> records) {
        records.forEach(record -> record.getRedisDataEntry().pushMessage(jedisCluster, redisTTL));
        return new ArrayList<>();
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedisCluster.close();
    }
}
