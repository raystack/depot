package io.odpf.depot.redis.dataentry;

import lombok.Getter;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

public class RedisStandaloneResponse implements RedisResponse {
    private final Response response;
    @Getter
    private final long index;
    @Getter
    private String message;
    @Getter
    private boolean failed = true;

    public RedisStandaloneResponse(Response response, long index) {
        this.index = index;
        this.response = response;
    }

    public RedisStandaloneResponse process() {
        try {
            Object o = response.get();
            message = o.toString();
            failed = false;
        } catch (JedisException e) {
            message = e.getMessage();
            failed = true;
        }
        return this;
    }
}
