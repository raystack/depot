package io.odpf.depot.redis.client;

import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.exception.NoResponseException;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis standalone client.
 */
public class RedisStandaloneClient implements RedisClient {

    private Instrumentation instrumentation;
    private RedisTtl redisTTL;
    private Jedis jedis;
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
    public List<OdpfMessage> execute(List<RedisRecord> records) {
        jedisPipelined = jedis.pipelined();
        jedisPipelined.multi();
        records.forEach(record -> record.getRedisDataEntry().pushMessage(jedisPipelined, redisTTL));
        Response<List<Object>> responses = jedisPipelined.exec();
        instrumentation.logDebug("jedis responses: {}", responses);
        jedisPipelined.sync();
        if (responses.get() == null || responses.get().isEmpty()) {
            throw new NoResponseException();
        }
        return new ArrayList<>();
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedis.close();
    }
}
