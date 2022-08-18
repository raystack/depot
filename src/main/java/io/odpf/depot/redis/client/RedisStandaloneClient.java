package io.odpf.depot.redis.client;

import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.dataentry.RedisResponse;
import io.odpf.depot.redis.dataentry.RedisStandaloneResponse;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.List;
import java.util.stream.Collectors;

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

    /**
     * Pushes records in a transaction.
     * if the transaction fails, whole batch should be retried.
     *
     * @param records records to send
     * @return Custom response
     */
    @Override
    public List<RedisResponse> execute(List<RedisRecord> records) {
        jedisPipelined = jedis.pipelined();
        jedisPipelined.multi();
        List<RedisStandaloneResponse> responses = records.stream()
                .map(redisRecord -> redisRecord.getRedisDataEntry().pushMessage(jedisPipelined, redisTTL))
                .collect(Collectors.toList());
        Response<List<Object>> r = jedisPipelined.exec();
        jedisPipelined.sync();
        instrumentation.logDebug("jedis responses: {}", r.get());
        return responses.stream().map(RedisStandaloneResponse::process).filter(RedisStandaloneResponse::isFailed).collect(Collectors.toList());
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedis.close();
    }
}
