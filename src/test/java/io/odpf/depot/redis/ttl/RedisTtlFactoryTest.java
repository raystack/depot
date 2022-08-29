package io.odpf.depot.redis.ttl;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.redis.enums.RedisSinkTtlType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;
@RunWith(MockitoJUnitRunner.class)
public class RedisTtlFactoryTest {

    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DISABLE);
    }

    @Test
    public void shouldReturnNoTTLIfNothingGiven() {
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), NoRedisTtl.class);
    }

    @Test
    public void shouldReturnExactTimeTTL() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.EXACT_TIME);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(100L);
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), ExactTimeTtl.class);
    }

    @Test
    public void shouldReturnDurationTTL() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(100L);
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), DurationTtl.class);
    }

    @Test
    public void shouldThrowExceptionInCaseOfInvalidConfiguration() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Provide a positive TTL value");
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(-1L);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        RedisTTLFactory.getTTl(redisSinkConfig);
    }
}
