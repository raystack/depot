package io.odpf.depot.redis;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.RedisClient;
import io.odpf.depot.redis.dataentry.RedisResponse;
import io.odpf.depot.redis.dataentry.RedisStandaloneResponse;
import io.odpf.depot.redis.models.RedisRecords;
import io.odpf.depot.redis.parsers.RedisParser;
import io.odpf.depot.redis.parsers.RedisResponseParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
        RedisRecords records = redisParser.convert(messages);
        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        records.getInvalidRecords().forEach(invalidRecord -> odpfSinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (records.getValidRecords().size() > 0) {
            List<RedisResponse> failedResponses = redisClient.execute(records.getValidRecords());
            Map<Long, ErrorInfo> errorInfoMap = RedisResponseParser.parse(records.getValidRecords(), failedResponses, instrumentation);
            errorInfoMap.forEach(odpfSinkResponse::addErrors);
            instrumentation.logInfo("Pushed a batch of {} records to Redis", records.getValidRecords().size());
        }
        return odpfSinkResponse;
    }

    @Override
    public void close() throws IOException {

    }
}
