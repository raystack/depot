package org.raystack.depot.redis.util;

import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.redis.client.entry.RedisListEntry;
import org.raystack.depot.redis.client.response.RedisClusterResponse;
import org.raystack.depot.redis.client.response.RedisResponse;
import org.raystack.depot.redis.record.RedisRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkUtilsTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Test
    public void shouldGetErrorsFromResponse() {
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 7L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 10L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 15L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(new RedisClusterResponse("LPUSH", "OK", null));
        responses.add(new RedisClusterResponse("FAILED AT 4"));
        responses.add(new RedisClusterResponse("FAILED AT 7"));
        responses.add(new RedisClusterResponse("FAILED AT 10"));
        responses.add(new RedisClusterResponse("LPUSH", "OK", null));
        Map<Long, ErrorInfo> errors = RedisSinkUtils.getErrorsFromResponse(records, responses,
                new Instrumentation(statsDReporter, RedisSinkUtils.class));
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals("FAILED AT 4", errors.get(4L).getException().getMessage());
        Assert.assertEquals("FAILED AT 7", errors.get(7L).getException().getMessage());
        Assert.assertEquals("FAILED AT 10", errors.get(10L).getException().getMessage());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(4L).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(10L).getErrorType());
    }

    @Test
    public void shouldGetEmptyMapWhenNoErrors() {
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 7L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 10L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 15L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.forEach(response -> {
            Mockito.when(response.isFailed()).thenReturn(false);
        });
        Map<Long, ErrorInfo> errors = RedisSinkUtils.getErrorsFromResponse(records, responses,
                new Instrumentation(statsDReporter, RedisSinkUtils.class));
        Assert.assertTrue(errors.isEmpty());
    }
}
