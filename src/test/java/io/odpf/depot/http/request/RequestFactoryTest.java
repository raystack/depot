package io.odpf.depot.http.request;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpRequestBodyType;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.enums.HttpRequestType;
import io.odpf.depot.message.OdpfMessageParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Properties;

@RunWith(MockitoJUnitRunner.class)
public class RequestFactoryTest {

    @Mock
    private HttpSinkConfig sinkConfig;
    @Mock
    private OdpfMessageParser odpfMessageParser;

    @Test
    public void shouldReturnSingleRequest() throws IOException {
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Mockito.when(sinkConfig.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
        Mockito.when(sinkConfig.getSinkHttpHeadersTemplate()).thenReturn(new Properties());
        Request request = RequestFactory.create(sinkConfig, odpfMessageParser);
        Assert.assertTrue(request instanceof SingleRequest);
    }

    @Test
    public void shouldReturnBatchRequest() throws IOException {
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.BATCH);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Mockito.when(sinkConfig.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
        Mockito.when(sinkConfig.getSinkHttpHeadersTemplate()).thenReturn(new Properties());
        Request request = RequestFactory.create(sinkConfig, odpfMessageParser);
        Assert.assertTrue(request instanceof BatchRequest);
    }
}
