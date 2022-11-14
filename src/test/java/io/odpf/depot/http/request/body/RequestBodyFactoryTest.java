package io.odpf.depot.http.request.body;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpRequestBodyType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestBodyFactoryTest {

    @Mock
    private HttpSinkConfig sinkConfig;

    @Test
    public void shouldReturnRawBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof RawBody);
    }

    @Test
    public void shouldReturnJsonBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.JSON);
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof JsonBody);
    }

    @Test
    public void shouldReturnMessageBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.MESSAGE);
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof MessageBody);
    }

    @Test
    public void shouldReturnTemplatizedJsonBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.TEMPLATIZED_JSON);
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof TemplatizedJsonBody);
    }
}
