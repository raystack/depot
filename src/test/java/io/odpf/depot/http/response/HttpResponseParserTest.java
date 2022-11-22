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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpResponseParserTest {

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private HttpEntityEnclosingRequestBase request;

    @Test
    public void shouldGetErrorsFromResponse() {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(new HttpRequestRecord(Collections.singletonList(0), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(1), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(4), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(7), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(12), null, true, request));

        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
        Mockito.when(request.getEntity()).thenReturn(httpEntity);

        HttpResponse successHttpResponse = Mockito.mock(HttpResponse.class);
        StatusLine successStatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        Mockito.when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = Mockito.mock(HttpResponse.class);
        StatusLine failedStatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        Mockito.when(failedStatusLine.getStatusCode()).thenReturn(500);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(successHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
            add(new HttpSinkResponse(failedHttpResponse));
        }};

        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(12L).getErrorType());
    }

    @Test
    public void shouldGetEmptyMapWhenNoErrors() {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(new HttpRequestRecord(Collections.singletonList(0), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(1), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(4), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(7), null, true, request));
        records.add(new HttpRequestRecord(Collections.singletonList(12), null, true, request));

        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));

        IntStream.range(0, responses.size()).forEach(
                index -> {
                    when(responses.get(index).isFailed()).thenReturn(false);
                }
        );
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
        Mockito.when(request.getEntity()).thenReturn(httpEntity);
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, instrumentation);
        Assert.assertTrue(errors.isEmpty());
    }
}
