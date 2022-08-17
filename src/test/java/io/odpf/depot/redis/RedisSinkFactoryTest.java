package io.odpf.depot.redis;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.enums.RedisSinkDataType;
import io.odpf.depot.redis.enums.RedisSinkTtlType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private RedisSinkConfig redisSinkConfig;
    private RedisSinkFactory redisSinkFactory;

    @Before
    public void setup() {
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("localhost:6379");
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("test-value");
        when(redisSinkConfig.getSinkRedisListDataProtoIndex()).thenReturn("test-value");
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DISABLE);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(0L);
        when(redisSinkConfig.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        redisSinkFactory = new RedisSinkFactory(redisSinkConfig, statsDReporter);
    }

    @Test
    public void shouldCreateRedisSink() {
        redisSinkFactory.init();
        OdpfSink sink = redisSinkFactory.create();
        assertEquals(RedisSink.class, sink.getClass());
    }
}
