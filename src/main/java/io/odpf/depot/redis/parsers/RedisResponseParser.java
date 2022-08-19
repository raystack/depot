package io.odpf.depot.redis.parsers;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.models.RedisRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RedisResponseParser {

    public static Map<Long, ErrorInfo> parse(List<RedisRecord> redisRecords, List<RedisResponse> responses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> {
                    RedisResponse response = responses.get(index);
                    if (response.isFailed()) {
                        RedisRecord record = redisRecords.get(index);
                        instrumentation.logError("Error while bigquery insert for message. Record: {}, Error: {}",
                                record.toString(), response.getMessage());
                        errors.put(record.getIndex(), new ErrorInfo(new Exception(response.getMessage()), ErrorType.DEFAULT_ERROR));
                    }
                }
        );
        return errors;
    }
}
