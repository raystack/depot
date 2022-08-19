package io.odpf.depot.redis.client.response;

import lombok.Getter;

public class RedisClusterResponse implements RedisResponse {
    @Getter
    private final String message;
    @Getter
    private final boolean failed;

    public RedisClusterResponse(String message, boolean failed) {
        this.message = message;
        this.failed = failed;
    }
}
