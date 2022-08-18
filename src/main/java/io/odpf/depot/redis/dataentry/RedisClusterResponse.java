package io.odpf.depot.redis.dataentry;

import lombok.Getter;

public class RedisClusterResponse implements RedisResponse {
    @Getter
    private final long index;
    @Getter
    private final String message;
    @Getter
    private final boolean failed;

    public RedisClusterResponse(long index, String message, boolean failed) {
        this.index = index;
        this.message = message;
        this.failed = failed;
    }
}
