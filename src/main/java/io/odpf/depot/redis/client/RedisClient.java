package io.odpf.depot.redis.client;

import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.redis.models.RedisRecord;

import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient {
    List<OdpfMessage> execute(List<RedisRecord> records);

    void close();
}
