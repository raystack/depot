package io.odpf.depot.redis.dataentry;

public interface RedisResponse {
    long getIndex();

    String getMessage();

    boolean isFailed();
}
