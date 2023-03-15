package com.gotocompany.depot.http.request;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;
import com.gotocompany.depot.http.enums.HttpRequestType;
import com.gotocompany.depot.message.MessageParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestFactoryTest {

    @Mock
    private HttpSinkConfig sinkConfig;
    @Mock
    private MessageParser messageParser;

    @Test
    public void shouldReturnSingleRequest() throws InvalidTemplateException {
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn("http://dummy.com");
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(sinkConfig, messageParser);
        Assert.assertTrue(request instanceof SingleRequest);
    }

    @Test
    public void shouldReturnBatchRequest() throws InvalidTemplateException {
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn("http://dummy.com");
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.BATCH);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(sinkConfig, messageParser);
        Assert.assertTrue(request instanceof BatchRequest);
    }
}
