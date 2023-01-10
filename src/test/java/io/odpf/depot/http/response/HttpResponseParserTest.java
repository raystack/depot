package io.odpf.depot.http.response;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.metrics.Instrumentation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpResponseParserTest {

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private HttpEntityEnclosingRequestBase request;

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

        Mockito.when(request.getEntity()).thenReturn(entity);

        HttpResponse successHttpResponse = Mockito.mock(HttpResponse.class);
        StatusLine successStatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        Mockito.when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = Mockito.mock(HttpResponse.class);
        StatusLine failedStatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        Mockito.when(failedStatusLine.getStatusCode()).thenReturn(500);
        Mockito.when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();

        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(12L).getErrorType());
        Mockito.verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Record: {}, Response Code: {}, Response Body: {}", null, "500", null);
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
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();

        IntStream.range(0, responses.size()).forEach(
                index -> {
                    when(responses.get(index).isFailed()).thenReturn(false);
                }
        );
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, instrumentation);
        Assert.assertTrue(errors.isEmpty());
    }

    @Test
    public void shouldGetSinkRetryableErrorWhenStatusCodeFallsUnderConfiguredRange() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        Mockito.when(request.getEntity()).thenReturn(entity);

        HttpResponse successHttpResponse = Mockito.mock(HttpResponse.class);
        StatusLine successStatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        Mockito.when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = Mockito.mock(HttpResponse.class);
        StatusLine failedStatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        Mockito.when(failedStatusLine.getStatusCode()).thenReturn(500);
        Mockito.when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        retryStatusCodeRanges.put(500, true);

        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(12L).getErrorType());
        Mockito.verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Record: {}, Response Code: {}, Response Body: {}", null, "500", null);

    }

    private HttpRequestRecord createRecord(Integer index) {
        HttpRequestRecord record = new HttpRequestRecord(null, true, request);
        record.addIndex(index);
        return record;
    }
}
