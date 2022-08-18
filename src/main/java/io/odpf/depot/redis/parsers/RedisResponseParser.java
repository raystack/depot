package io.odpf.depot.redis.parsers;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.dataentry.RedisResponse;
import io.odpf.depot.redis.dataentry.RedisStandaloneResponse;
import io.odpf.depot.redis.models.RedisRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisResponseParser {

    public static Map<Long, ErrorInfo> parse(List<RedisRecord> validRecords, List<RedisResponse> failedResponses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        if (failedResponses.size() == 0) {
            return errors;
        }
        for (RedisResponse r : failedResponses) {
            RedisRecord record = validRecords.get((int) r.getIndex());
            instrumentation.logError("Error while bigquery insert for message. Record: {}, Error: {}, MetaData: {}",
                    record.getRedisDataEntry(), r.getMessage(), record.getMetadata());
            errors.put(r.getIndex(), new ErrorInfo(new Exception(r.getMessage()), ErrorType.DEFAULT_ERROR));
        }
        return errors;
    }
}
