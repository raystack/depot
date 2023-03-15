package com.gotocompany.depot.redis.client.response;

public interface RedisResponse {
    String getMessage();

    boolean isFailed();
}
