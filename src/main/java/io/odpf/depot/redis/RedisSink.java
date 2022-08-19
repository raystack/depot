package io.odpf.depot.redis;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.RedisClient;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.parsers.RedisParser;
import io.odpf.depot.redis.parsers.RedisResponseParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisSink implements OdpfSink {
    private final RedisClient redisClient;
    private final RedisParser redisParser;
    private final Instrumentation instrumentation;

    public RedisSink(RedisClient redisClient, RedisParser redisParser, Instrumentation instrumentation) {
        this.redisClient = redisClient;
        this.redisParser = redisParser;
        this.instrumentation = instrumentation;
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) {
        List<RedisRecord> records = redisParser.convert(messages);
        Map<Boolean, List<RedisRecord>> splitterRecords = records.stream().collect(Collectors.partitioningBy(RedisRecord::isValid));
        List<RedisRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<RedisRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        invalidRecords.forEach(invalidRecord -> odpfSinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (validRecords.size() > 0) {
            List<RedisResponse> responses = redisClient.send(validRecords);
            if (responses.stream().anyMatch(RedisResponse::isFailed)) {
                Map<Long, ErrorInfo> errorInfoMap = RedisResponseParser.parse(validRecords, responses, instrumentation);
                errorInfoMap.forEach(odpfSinkResponse::addErrors);
            }
            instrumentation.logInfo("Pushed a batch of {} records to Redis", validRecords.size());
        }
        return odpfSinkResponse;
    }

    @Override
    public void close() throws IOException {

    }
}
