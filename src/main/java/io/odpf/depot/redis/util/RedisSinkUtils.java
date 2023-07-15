package org.raystack.depot.redis.util;

import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.redis.client.response.RedisResponse;
import org.raystack.depot.redis.record.RedisRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RedisSinkUtils {
    public static Map<Long, ErrorInfo> getErrorsFromResponse(List<RedisRecord> redisRecords,
            List<RedisResponse> responses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> {
                    RedisResponse response = responses.get(index);
                    if (response.isFailed()) {
                        RedisRecord record = redisRecords.get(index);
                        instrumentation.logError("Error while inserting to redis for message. Record: {}, Error: {}",
                                record.toString(), response.getMessage());
                        errors.put(record.getIndex(),
                                new ErrorInfo(new Exception(response.getMessage()), ErrorType.DEFAULT_ERROR));
                    }
                });
        return errors;
    }
}
