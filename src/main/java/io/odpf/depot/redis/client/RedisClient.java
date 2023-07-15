package org.raystack.depot.redis.client;

import org.raystack.depot.redis.client.response.RedisResponse;
import org.raystack.depot.redis.record.RedisRecord;

import java.io.Closeable;
import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient extends Closeable {
    List<RedisResponse> send(List<RedisRecord> records);
}
