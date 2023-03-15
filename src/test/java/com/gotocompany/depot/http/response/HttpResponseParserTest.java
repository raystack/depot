package com.gotocompany.depot.http.response;

import com.gotocompany.depot.config.converter.RangeToHashMapConverter;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpResponseParserTest {

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private HttpEntity entity;

    @Test
    public void shouldGetErrorsFromResponse() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse successHttpResponse = mock(HttpResponse.class);
        StatusLine successStatusLine = mock(StatusLine.class);
        when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(7L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(12L).getErrorType());
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", "500", null);
    }

    @Test
    public void shouldGetEmptyMapWhenNoErrors() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> when(responses.get(index).getResponseCode()).thenReturn("500")
        );
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        Assert.assertTrue(errors.isEmpty());
    }

    @Test
    public void shouldLogRequestIfResponseCodeInStatusCodeRanges() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};
        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        assertEquals(3, errors.size());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(0L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(4L).getErrorType());
        verify(instrumentation, times(3)).logInfo(
                "\nRequest Method: PUT"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]"
        );
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", "500", null);
    }

    @Test
    public void shouldNotLogRequestIfResponseCodeIsNotInStatusCodeRanges() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(400);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};
        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        assertEquals(3, errors.size());
        assertEquals(ErrorType.SINK_4XX_ERROR, errors.get(0L).getErrorType());
        assertEquals(ErrorType.SINK_4XX_ERROR, errors.get(1L).getErrorType());
        assertEquals(ErrorType.SINK_4XX_ERROR, errors.get(4L).getErrorType());
        verify(instrumentation, times(0)).logInfo(
                "\nRequest Method: PUT"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]"
        );
    }

    @Test
    public void shouldGetSinkRetryableErrorWhenStatusCodeFallsUnderConfiguredRange() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse successHttpResponse = mock(HttpResponse.class);
        StatusLine successStatusLine = mock(StatusLine.class);
        when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        retryStatusCodeRanges.put(500, true);

        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(12L).getErrorType());
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", "500", null);
    }

    @Test
    public void shouldNotGetSinkRetryableErrorWhenStatusCodeIsNotUnderConfiguredRange() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse successHttpResponse = mock(HttpResponse.class);
        StatusLine successStatusLine = mock(StatusLine.class);
        when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        retryStatusCodeRanges.put(501, true);

        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(12L).getErrorType());
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", "500", null);
    }

    private HttpRequestRecord createRecord(Integer index) {
        HttpEntityEnclosingRequestBase request = new HttpPut("http://dummy.com");
        request.setEntity(new StringEntity("[{\"key\":\"value1\"},{\"key\":\"value2\"}]", ContentType.APPLICATION_JSON));
        request.setHeader(new BasicHeader("Accept", "text/plain"));
        HttpRequestRecord record = new HttpRequestRecord(null, true, request);
        record.addIndex(index);
        return record;
    }

    private Map<Integer, Boolean> createRequestLogStatusCode() {
        return new RangeToHashMapConverter().convert(null, "401-600");
    }
}
