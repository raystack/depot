package io.odpf.depot.redis.models;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RedisRecord {
    private final RedisDataEntry redisDataEntry;
    private final Long index;
    private final ErrorInfo errorInfo;
}
