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
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, false);
        Assert.assertFalse(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
    }

    @Test
    public void shouldReportWhenSuccess() {
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, true);
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
    }

    @Test
    public void shouldGetResponseCodeIfSuccess() {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, false);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertFalse(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
        Assert.assertEquals("200", responseCode);
    }

    @Test
    public void shouldGetResponseCodeIfNotSuccess() {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(400);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, true);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
        Assert.assertEquals("400", responseCode);
    }

    @Test
    public void shouldReturnNullResponseCodeIfResponseIsNull() {
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, true);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
        Assert.assertEquals("null", responseCode);
    }

    @Test
    public void shouldReturnNullResponseCodeIfStatusLineIsNull() {
        Mockito.when(response.getStatusLine()).thenReturn(null);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, true);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
        Assert.assertEquals("null", responseCode);
    }

    @Test
    public void shouldGetResponseBody() throws IOException {
        String body = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(httpEntity.getContent()).thenReturn(new StringInputStream(body));

        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, true);

        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals(body, httpSinkResponse.getResponseBody());
    }
}
