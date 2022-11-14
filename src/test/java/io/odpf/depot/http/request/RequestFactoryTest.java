package io.odpf.depot.http.request;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.http.enums.HttpRequestBodyType;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.enums.HttpRequestType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestFactoryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private HttpSinkConfig sinkConfig;

    @Test
    public void shouldReturnSingleRequest() {
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn("http://dummy.com");
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Mockito.when(sinkConfig.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
        Request request = RequestFactory.create(sinkConfig);
        Assert.assertTrue(request instanceof SingleRequest);
    }

    @Test
    public void shouldReturnBatchRequest() {
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn("http://dummy.com");
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.BATCH);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Mockito.when(sinkConfig.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
        Request request = RequestFactory.create(sinkConfig);
        Assert.assertTrue(request instanceof BatchRequest);
    }

    @Test
    public void shouldFailWhenUrlConfigIsEmpty() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Service URL '' is invalid");
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn("");
        RequestFactory.create(sinkConfig);
    }

    @Test
    public void shouldFailWhenUrlConfigIsNull() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Service URL 'null' is invalid");
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn(null);
        RequestFactory.create(sinkConfig);
    }
}
