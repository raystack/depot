package io.odpf.depot.redis.client;

import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.exception.NoResponseException;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.List;

/**
 * Redis standalone client.
 */
public class RedisStandaloneClient implements RedisClient {

    private final Instrumentation instrumentation;
    private final RedisTtl redisTTL;
    private final Jedis jedis;
    private Pipeline jedisPipelined;

    /**
     * Instantiates a new Redis standalone client.
     *
     * @param instrumentation the instrumentation
     * @param redisTTL        the redis ttl
     * @param jedis           the jedis
     */
    public RedisStandaloneClient(Instrumentation instrumentation, RedisTtl redisTTL, Jedis jedis) {
        this.instrumentation = instrumentation;
        this.redisTTL = redisTTL;
        this.jedis = jedis;
    }

    @Override
    public Response execute(List<RedisRecord> records) {
        jedisPipelined = jedis.pipelined();
        jedisPipelined.multi();
        records.forEach(record -> record.getRedisDataEntry().pushMessage(jedisPipelined, redisTTL));
        Response<List<Object>> responses = jedisPipelined.exec();
        instrumentation.logDebug("jedis responses: {}", responses);
        jedisPipelined.sync();
        return responses;
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedis.close();
    }
}
