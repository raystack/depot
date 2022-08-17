package io.odpf.depot.redis.client;

import io.odpf.depot.redis.models.RedisRecord;
import redis.clients.jedis.Response;

import java.io.Closeable;
import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient extends Closeable {
    Response execute(List<RedisRecord> records);
}
