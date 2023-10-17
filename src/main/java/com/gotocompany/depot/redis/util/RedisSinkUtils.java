package com.gotocompany.depot.redis.util;

import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.Instrumentation;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RedisSinkUtils {
    public static Map<Long, ErrorInfo> getErrorsFromResponse(List<RedisRecord> redisRecords, List<RedisResponse> responses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> {
                    RedisResponse response = responses.get(index);
                    if (response.isFailed()) {
                        RedisRecord record = redisRecords.get(index);
                        instrumentation.logError("Error while inserting to redis for message. Record: {}, Error: {}",
                                record.toString(), response.getMessage());
                        errors.put(record.getIndex(), new ErrorInfo(new Exception(response.getMessage()), ErrorType.DEFAULT_ERROR));
                    }
                }
        );
        return errors;
    }

    public static Map<Long, ErrorInfo> getNonRetryableErrors(List<RedisRecord> redisRecords, JedisConnectionException e, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (RedisRecord record : redisRecords) {
            instrumentation.logError("Error while inserting to redis for message. Record: {}, Error: {}",
                    record.toString(), e.getMessage());
            errors.put(record.getIndex(), new ErrorInfo(new Exception(e.getMessage()), ErrorType.SINK_4XX_ERROR));

        }
        return errors;
    }
}
