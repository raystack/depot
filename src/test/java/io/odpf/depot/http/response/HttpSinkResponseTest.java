package io.odpf.depot.http.response;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkResponseTest {

    @Mock
    private HttpResponse response;

    @Mock
    private HttpEntity httpEntity;

    @Mock
    private StatusLine statusLine;

    @Test
    public void shouldReportWhenFailed() {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        Assert.assertTrue(httpSinkResponse.isFailed());
    }

    @Test
    public void shouldReportWhenSuccess() {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        Assert.assertFalse(httpSinkResponse.isFailed());
    }

    @Test
    public void shouldGetResponseCodeIfSuccess() {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertFalse(httpSinkResponse.isFailed());
        Assert.assertEquals("200", responseCode);
    }

    @Test
    public void shouldGetResponseCodeIfNotSuccess() {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals("500", responseCode);
    }

    @Test
    public void shouldReturnNullResponseCodeIfResponseIsNull() {
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals("null", responseCode);
    }

    @Test
    public void shouldReturnNullResponseCodeIfStatusLineIsNull() {
        Mockito.when(response.getStatusLine()).thenReturn(null);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals("null", responseCode);
    }

    @Test
    public void shouldGetResponseBody() throws IOException {
        String body = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(httpEntity.getContent()).thenReturn(new StringInputStream(body));

        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);

        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals(body, httpSinkResponse.getResponseBody());
    }
}
