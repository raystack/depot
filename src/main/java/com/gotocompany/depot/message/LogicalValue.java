package com.gotocompany.depot.message;

import com.gotocompany.depot.schema.LogicalType;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public interface LogicalValue {
    LogicalType getType();
    Instant getTimestamp();
    Map<String, Object> getStruct();
    Duration getDuration();
}
