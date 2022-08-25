package io.odpf.depot.redis.client.response;

import lombok.Getter;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

public class RedisStandaloneResponse implements RedisResponse {
    private final Response response;
    private final Response ttlResponse;
    private final String command;
    @Getter
    private String message;
    @Getter
    private boolean failed = true;

    public RedisStandaloneResponse(String command, Response response, Response ttlResponse) {
        this.command = command;
        this.response = response;
        this.ttlResponse = ttlResponse;
    }

    public RedisStandaloneResponse process() {
        try {
            Object cmd = response.get();
            Object ttl = ttlResponse != null ? (((long) ttlResponse.get()) == 0L ? "NOT UPDATED" : "UPDATED") :"NoOp";
            message = String.format("%s: %s, TTL: %s", command, cmd, ttl);
            failed = false;
        } catch (JedisException e) {
            message = e.getMessage();
            failed = true;
        }
        return this;
    }
}
