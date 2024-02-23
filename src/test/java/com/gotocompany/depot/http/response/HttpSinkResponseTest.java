package com.gotocompany.depot.http.response;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
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
    public void shouldReportWhenFailed() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        Assert.assertTrue(httpSinkResponse.isFail());
    }

    @Test
    public void shouldReportWhenSuccess() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        Assert.assertFalse(httpSinkResponse.isFail());
    }

    @Test
    public void shouldGetResponseCodeIfSuccess() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertFalse(httpSinkResponse.isFail());
        Assert.assertEquals("200", responseCode);
    }

    @Test
    public void shouldGetResponseCodeIfNotSuccess() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertEquals("500", responseCode);
    }

    @Test
    public void shouldReturnNullResponseCodeIfResponseIsNull() throws IOException {
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertEquals("null", responseCode);
    }

    @Test
    public void shouldReturnNullResponseCodeIfStatusLineIsNull() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(null);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);
        String responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertEquals("null", responseCode);
    }

    @Test
    public void shouldGetResponseBody() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);

        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response);

        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertNull(httpSinkResponse.getResponseBody());
    }
}
