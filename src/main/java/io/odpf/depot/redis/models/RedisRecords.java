package io.odpf.depot.redis.models;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class RedisRecords {
    private final List<RedisRecord> validRecords;
    private final List<RedisRecord> invalidRecords;
}
