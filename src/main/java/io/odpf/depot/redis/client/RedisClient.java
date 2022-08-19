package io.odpf.depot.redis.client;

import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.models.RedisRecord;

import java.io.Closeable;
import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient extends Closeable {
    List<RedisResponse> send(List<RedisRecord> records);
}
