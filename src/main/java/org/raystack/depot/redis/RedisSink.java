package org.raystack.depot.redis;

import org.raystack.depot.message.Message;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.redis.client.RedisClient;
import org.raystack.depot.redis.client.response.RedisResponse;
import org.raystack.depot.redis.parsers.RedisParser;
import org.raystack.depot.redis.record.RedisRecord;
import org.raystack.depot.redis.util.RedisSinkUtils;
import org.raystack.depot.Sink;
import org.raystack.depot.SinkResponse;
import org.raystack.depot.error.ErrorInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisSink implements Sink {
    private final RedisClient redisClient;
    private final RedisParser redisParser;
    private final Instrumentation instrumentation;

    public RedisSink(RedisClient redisClient, RedisParser redisParser, Instrumentation instrumentation) {
        this.redisClient = redisClient;
        this.redisParser = redisParser;
        this.instrumentation = instrumentation;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        List<RedisRecord> records = redisParser.convert(messages);
        Map<Boolean, List<RedisRecord>> splitterRecords = records.stream()
                .collect(Collectors.partitioningBy(RedisRecord::isValid));
        List<RedisRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<RedisRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        SinkResponse sinkResponse = new SinkResponse();
        invalidRecords.forEach(
                invalidRecord -> sinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (validRecords.size() > 0) {
            List<RedisResponse> responses = redisClient.send(validRecords);
            Map<Long, ErrorInfo> errorInfoMap = RedisSinkUtils.getErrorsFromResponse(validRecords, responses,
                    instrumentation);
            errorInfoMap.forEach(sinkResponse::addErrors);
            instrumentation.logInfo("Pushed a batch of {} records to Redis", validRecords.size());
        }
        return sinkResponse;
    }

    @Override
    public void close() throws IOException {

    }
}
