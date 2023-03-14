package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.record.RedisRecord;

import java.io.Closeable;
import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient extends Closeable {
    List<RedisResponse> send(List<RedisRecord> records);
}
