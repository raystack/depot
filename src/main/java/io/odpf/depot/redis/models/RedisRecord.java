package io.odpf.depot.redis.models;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class RedisRecord {
    @Getter
    private final RedisDataEntry redisDataEntry;
    private final Long index;
    private final ErrorInfo errorInfo;
    @Getter
    private Map<String, Object> metadata;
}
