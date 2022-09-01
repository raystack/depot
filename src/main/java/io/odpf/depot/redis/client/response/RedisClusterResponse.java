package io.odpf.depot.redis.client.response;

import lombok.Getter;

public class RedisClusterResponse implements RedisResponse {
    @Getter
    private final String message;
    @Getter
    private final boolean failed;

    public RedisClusterResponse(String command, Object response, Long ttlResponse) {
        this.message = String.format(
                "%s: %s, TTL: %s",
                command,
                response,
                ttlResponse == null ? "NoOp" : ttlResponse == 0 ? "NOT UPDATED" : "UPDATED");
        this.failed = false;
    }

    public RedisClusterResponse(String message) {
        this.message = message;
        this.failed = true;
    }
}
