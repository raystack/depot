package io.odpf.depot.http.request;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpRequestBodyType;
import io.odpf.depot.http.enums.HttpRequestType;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SingleRequestTest {

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private UriBuilder uriBuilder;

    @Mock
    private RequestBody requestBody;

    @Mock
    private HttpSinkConfig config;

    @Test
    public void shouldProduceSingleRequests() {
        when(config.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(config);
        assertTrue(request instanceof SingleRequest);
    }

    @Test
    public void shouldProduceSingleRequestsWhenPutRequest() {
        when(config.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(config);
    }

    @Test
    public void shouldProduceSingleRequestsWhenPostRequest() {
        when(config.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(config);
        assertTrue(request instanceof SingleRequest);
    }

    @Test
    public void shouldProduceSingleRequestsWhenPatchRequest() {
        when(config.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(config);
        assertTrue(request instanceof SingleRequest);
    }
}
