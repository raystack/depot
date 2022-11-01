package io.odpf.depot.http.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.response.HttpSinkResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpRequestRecordTest {

    @Mock
    private HttpEntityEnclosingRequestBase httpRequest;

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponse httpResponse;

    @Mock
    private HttpEntity httpEntity;

    @Mock
    private StatusLine statusLine;

    @Test
    public void shouldGetRecordIndex() {
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        assertEquals(new Long(0), httpRequestRecord.getIndex());
    }

    @Test
    public void shouldGetRecordErrorInfo() {
        ErrorInfo errorInfo = new ErrorInfo(new Exception(""), ErrorType.DEFAULT_ERROR);
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, errorInfo, true);
        assertEquals(errorInfo, httpRequestRecord.getErrorInfo());
    }

    @Test
    public void shouldGetValidRecord() {
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        assertTrue(httpRequestRecord.isValid());
    }

    @Test
    public void shouldGetInvalidRecord() {
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, new ErrorInfo(new Exception("error here"), ErrorType.DEFAULT_ERROR), false);
        assertFalse(httpRequestRecord.isValid());
    }

    @Test
    public void shouldSendHttpRequestWithSuccessResponse() throws IOException {
        when(httpClient.execute(httpRequest)).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        HttpRequestRecord requestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        HttpSinkResponse sinkResponse = requestRecord.send(httpClient);
        assertFalse(sinkResponse.isFailed());
    }

    @Test
    public void shouldSendHttpRequestWithFailedResponse() throws IOException {
        when(httpClient.execute(httpRequest)).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        HttpRequestRecord requestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        HttpSinkResponse sinkResponse = requestRecord.send(httpClient);
        assertTrue(sinkResponse.isFailed());
    }

    @Test
    public void shouldGetRequestBody() throws IOException {
        String body = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
        when(httpRequest.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream(body));
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        assertEquals(body, httpRequestRecord.getRequestBody());
    }

    @Test
    public void shouldGetNullStringIfRequestIsNull() {
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        assertEquals("null", httpRequestRecord.getRequestBody());
    }
}
