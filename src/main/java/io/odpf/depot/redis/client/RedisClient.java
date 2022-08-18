package io.odpf.depot.redis.client;

import io.odpf.depot.redis.dataentry.RedisResponse;
import io.odpf.depot.redis.dataentry.RedisStandaloneResponse;
import io.odpf.depot.redis.models.RedisRecord;

import java.io.Closeable;
import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient extends Closeable {
    List<RedisResponse> execute(List<RedisRecord> records);
}
