package io.odpf.depot.redis;

import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.RedisClient;
import io.odpf.depot.redis.client.entry.RedisListEntry;
import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.parsers.RedisParser;
import io.odpf.depot.redis.record.RedisRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {
    @Mock
    private RedisClient redisClient;
    @Mock
    private RedisParser redisParser;
    @Mock
    private Instrumentation instrumentation;

    @Test
    public void shouldPushToSink() {
        List<OdpfMessage> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 0L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 2L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        when(redisParser.convert(messages)).thenReturn(records);
        when(redisClient.send(records)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        OdpfSinkResponse odpfSinkResponse = redisSink.pushToSink(messages);
        Assert.assertFalse(odpfSinkResponse.hasErrors());
    }

    @Test
    public void shouldReportParsingErrors() {
        List<OdpfMessage> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(null, 0L, new ErrorInfo(new IOException(""), ErrorType.DESERIALIZATION_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(null, 2L, new ErrorInfo(new ConfigurationException(""), ErrorType.DEFAULT_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        when(redisParser.convert(messages)).thenReturn(records);
        List<RedisRecord> validRecords = records.stream().filter(RedisRecord::isValid).collect(Collectors.toList());
        when(redisClient.send(validRecords)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        OdpfSinkResponse odpfSinkResponse = redisSink.pushToSink(messages);
        Assert.assertTrue(odpfSinkResponse.hasErrors());
        Assert.assertEquals(2, odpfSinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, odpfSinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, odpfSinkResponse.getErrorsFor(2).getErrorType());
    }

    @Test
    public void shouldReportClientErrors() {
        List<OdpfMessage> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 0L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 2L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("failed at 2", true));
        responses.add(new RedisClusterResponse("failed at 3", true));
        responses.add(new RedisClusterResponse("failed at 4", true));
        when(redisParser.convert(messages)).thenReturn(records);
        List<RedisRecord> validRecords = records.stream().filter(RedisRecord::isValid).collect(Collectors.toList());
        when(redisClient.send(validRecords)).thenReturn(responses);
        when(redisClient.send(records)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        OdpfSinkResponse odpfSinkResponse = redisSink.pushToSink(messages);
        Assert.assertTrue(odpfSinkResponse.hasErrors());
        Assert.assertEquals(3, odpfSinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, odpfSinkResponse.getErrorsFor(2).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, odpfSinkResponse.getErrorsFor(3).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, odpfSinkResponse.getErrorsFor(4).getErrorType());
        Assert.assertEquals("failed at 2", odpfSinkResponse.getErrorsFor(2).getException().getMessage());
        Assert.assertEquals("failed at 3", odpfSinkResponse.getErrorsFor(3).getException().getMessage());
        Assert.assertEquals("failed at 4", odpfSinkResponse.getErrorsFor(4).getException().getMessage());
    }

    @Test
    public void shouldReportNetErrors() {
        List<OdpfMessage> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(null, 0L, new ErrorInfo(new IOException(""), ErrorType.DESERIALIZATION_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(null, 2L, new ErrorInfo(new ConfigurationException(""), ErrorType.DEFAULT_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("failed at 3", true));
        responses.add(new RedisClusterResponse("failed at 4", true));
        when(redisParser.convert(messages)).thenReturn(records);
        List<RedisRecord> validRecords = records.stream().filter(RedisRecord::isValid).collect(Collectors.toList());
        when(redisClient.send(validRecords)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        OdpfSinkResponse odpfSinkResponse = redisSink.pushToSink(messages);
        Assert.assertEquals(4, odpfSinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, odpfSinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, odpfSinkResponse.getErrorsFor(2).getErrorType());
        Assert.assertEquals("failed at 3", odpfSinkResponse.getErrorsFor(3).getException().getMessage());
        Assert.assertEquals("failed at 4", odpfSinkResponse.getErrorsFor(4).getException().getMessage());
    }
}
