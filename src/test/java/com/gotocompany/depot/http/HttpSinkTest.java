package com.gotocompany.depot.http;

import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.converter.RangeToHashMapConverter;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.http.client.HttpSinkClient;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.request.Request;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.message.Message;
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
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTest {

    @Mock
    private Request request;

    @Mock
    private HttpSinkClient httpSinkClient;

    @Mock
    private HttpResponse response;

    @Mock
    private HttpEntity httpEntity;

    @Mock
    private StatusLine statusLine;

    @Mock
    private Instrumentation instrumentation;

    @Test
    public void shouldPushToSink() throws IOException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, null, true));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        when(request.createRecords(messages)).thenReturn(records);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpSinkClient.send(records)).thenReturn(responses);

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        SinkResponse sinkResponse = httpSink.pushToSink(messages);
        Assert.assertFalse(sinkResponse.hasErrors());
    }

    @Test
    public void shouldReportParsingErrors() throws IOException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR), false));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR), false));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        when(request.createRecords(messages)).thenReturn(records);
        List<HttpRequestRecord> validRecords = records.stream().filter(HttpRequestRecord::isValid).collect(Collectors.toList());
        when(httpSinkClient.send(validRecords)).thenReturn(responses);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        SinkResponse sinkResponse = httpSink.pushToSink(messages);
        Assert.assertTrue(sinkResponse.hasErrors());
        Assert.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, sinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, sinkResponse.getErrorsFor(2).getErrorType());
    }

    @Test
    public void shouldReportErrors() throws IOException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, null, true));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));

        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(response.getEntity()).thenReturn(httpEntity);
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));
        responses.add(new HttpSinkResponse(response));

        when(request.createRecords(messages)).thenReturn(records);
        List<HttpRequestRecord> validRecords = records.stream().filter(HttpRequestRecord::isValid).collect(Collectors.toList());
        when(httpSinkClient.send(validRecords)).thenReturn(responses);
        when(httpSinkClient.send(records)).thenReturn(responses);

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        retryStatusCodeRanges.put(400, true);
        retryStatusCodeRanges.put(499, true);
        retryStatusCodeRanges.put(501, true);

        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        SinkResponse sinkResponse = httpSink.pushToSink(messages);
        Assert.assertTrue(sinkResponse.hasErrors());
        Assert.assertEquals(5, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrorsFor(1).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrorsFor(3).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrorsFor(4).getErrorType());
    }

    @Test
    public void shouldLogRequestBodyInDebugMode() throws SinkException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));

        when(request.createRecords(messages)).thenReturn(records);

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();

        retryStatusCodeRanges.put(501, true);
        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);

        when(request.createRecords(messages)).thenReturn(records);

        httpSink.pushToSink(messages);
        verify(instrumentation, times(1)).logInfo("Processed {} records to Http Service",
                1);
        verify(instrumentation, times(1)).logDebug("\nRequest Method: PUT\nRequest Url: http://dummy.com\nRequest Headers: [Accept: text/plain]\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]");

    }


    private HttpRequestRecord createRecord(Integer index, ErrorInfo errorInfo, boolean isValid) {
        HttpEntityEnclosingRequestBase httpRequest = new HttpPut("http://dummy.com");
        httpRequest.setEntity(new StringEntity("[{\"key\":\"value1\"},{\"key\":\"value2\"}]", ContentType.APPLICATION_JSON));
        httpRequest.setHeader(new BasicHeader("Accept", "text/plain"));
        HttpRequestRecord record = new HttpRequestRecord(errorInfo, isValid, httpRequest);
        record.addIndex(index);
        return record;
    }

    private Map<Integer, Boolean> createRequestLogStatusCode() {
        return new RangeToHashMapConverter().convert(null, "400-600");
    }

}
