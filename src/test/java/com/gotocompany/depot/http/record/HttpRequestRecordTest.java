package com.gotocompany.depot.http.record;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;
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
    public void shouldExactlyGetOneRecordIndex() {
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        Iterator<Integer> indexIterator = httpRequestRecord.iterator();
        assertTrue(indexIterator.hasNext());
        assertEquals(0, (int) indexIterator.next());
        assertFalse(indexIterator.hasNext());
    }

    @Test
    public void shouldGetRecordErrorInfo() {
        ErrorInfo errorInfo = new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR);
        HttpRequestRecord httpRequestRecord = createRecord(errorInfo, true);
        Assert.assertEquals(errorInfo, httpRequestRecord.getErrorInfo());
    }

    @Test
    public void shouldGetValidRecord() {
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        assertTrue(httpRequestRecord.isValid());
    }

    @Test
    public void shouldGetInvalidRecord() {
        ErrorInfo errorInfo = new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR);
        HttpRequestRecord httpRequestRecord = createRecord(errorInfo, false);
        assertFalse(httpRequestRecord.isValid());
    }

    @Test
    public void shouldSendHttpRequestWithSuccessResponse() throws IOException {
        when(httpClient.execute(httpRequest)).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        HttpRequestRecord requestRecord = createRecord(null, true);
        HttpSinkResponse sinkResponse = requestRecord.send(httpClient);
        assertFalse(sinkResponse.isFailed());
    }

    @Test
    public void shouldSendHttpRequestWithFailedResponse() throws IOException {
        when(httpClient.execute(httpRequest)).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        HttpRequestRecord requestRecord = createRecord(null, true);
        HttpSinkResponse sinkResponse = requestRecord.send(httpClient);
        assertTrue(sinkResponse.isFailed());
    }

    @Test
    public void shouldGetRequestBody() throws IOException {
        String body = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
        when(httpRequest.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream(body));
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        assertEquals(body, httpRequestRecord.getRequestBody());
    }

    @Test
    public void shouldGetNullIfRequestIsNull() throws IOException {
        when(httpRequest.getEntity()).thenReturn(httpEntity);
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        assertNull(httpRequestRecord.getRequestBody());
    }

    private HttpRequestRecord createRecord(ErrorInfo errorInfo, boolean isValid) {
        HttpRequestRecord record = new HttpRequestRecord(errorInfo, isValid, httpRequest);
        record.addIndex(0);
        return record;
    }
}
