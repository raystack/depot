package io.odpf.depot.http.response;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.metrics.Instrumentation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
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

    @Mock
    private HttpResponse response;

    @Mock
    private HttpEntity httpEntity;

    @Test
    public void shouldGetErrorsFromResponse() {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(new HttpRequestRecord(request, 0L, null, true));
        records.add(new HttpRequestRecord(request, 1L, null, true));
        records.add(new HttpRequestRecord(request, 4L, null, true));
        records.add(new HttpRequestRecord(request, 7L, null, true));
        records.add(new HttpRequestRecord(request, 12L, null, true));

        Mockito.when(request.getEntity()).thenReturn(httpEntity);
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(new HttpSinkResponse(response, false));
        responses.add(new HttpSinkResponse(response, true));
        responses.add(new HttpSinkResponse(response, false));
        responses.add(new HttpSinkResponse(response, true));
        responses.add(new HttpSinkResponse(response, true));

        Map<Long, ErrorInfo> errors = HttpResponseParser.parseAndFillError(records, responses, instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(12L).getErrorType());
    }

    @Test
    public void shouldGetEmptyMapWhenNoErrors() {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(new HttpRequestRecord(request, 0L, null, true));
        records.add(new HttpRequestRecord(request, 1L, null, true));
        records.add(new HttpRequestRecord(request, 4L, null, true));
        records.add(new HttpRequestRecord(request, 7L, null, true));
        records.add(new HttpRequestRecord(request, 12L, null, true));

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
        Map<Long, ErrorInfo> errors = HttpResponseParser.parseAndFillError(records, responses, instrumentation);
        Assert.assertTrue(errors.isEmpty());
    }
}
