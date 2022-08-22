package io.odpf.depot.redis.util;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;

import io.odpf.depot.redis.client.response.RedisClusterResponse;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.client.entry.RedisListEntry;
import io.odpf.depot.redis.record.RedisRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("FAILED AT 4", true));
        responses.add(new RedisClusterResponse("FAILED AT 7", true));
        responses.add(new RedisClusterResponse("FAILED AT 10", true));
        responses.add(new RedisClusterResponse("OK", false));
        Map<Long, ErrorInfo> errors = RedisSinkUtils.getErrorsFromResponse(records, responses, new Instrumentation(statsDReporter, RedisSinkUtils.class));
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals("FAILED AT 4", errors.get(4L).getException().getMessage());
        Assert.assertEquals("FAILED AT 7", errors.get(7L).getException().getMessage());
        Assert.assertEquals("FAILED AT 10", errors.get(10L).getException().getMessage());
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
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        responses.add(new RedisClusterResponse("OK", false));
        Map<Long, ErrorInfo> errors = RedisSinkUtils.getErrorsFromResponse(records, responses, new Instrumentation(statsDReporter, RedisSinkUtils.class));
        Assert.assertTrue(errors.isEmpty());
    }
}
