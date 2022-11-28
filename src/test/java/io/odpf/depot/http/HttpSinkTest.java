package io.odpf.depot.http;

import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.exception.InvalidMessageException;
import io.odpf.depot.http.client.HttpSinkClient;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.Request;
import io.odpf.depot.http.response.HttpSinkResponse;
import io.odpf.depot.message.OdpfMessage;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTest {

    @Mock
    private HttpEntityEnclosingRequestBase httpRequest;

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
        List<OdpfMessage> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, null, true));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        when(request.createRecords(messages)).thenReturn(records);
        when(httpSinkClient.send(records)).thenReturn(responses);
        HttpSink httpSink = new HttpSink(httpSinkClient, request, instrumentation);
        OdpfSinkResponse odpfSinkResponse = httpSink.pushToSink(messages);
        Assert.assertFalse(odpfSinkResponse.hasErrors());
    }

    @Test
    public void shouldReportParsingErrors() throws IOException {
        List<OdpfMessage> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, new ErrorInfo(new InvalidMessageException("Invalid Message Error"), ErrorType.INVALID_MESSAGE_ERROR), false));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR), false));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        responses.add(Mockito.mock(HttpSinkResponse.class));
        when(request.createRecords(messages)).thenReturn(records);
        List<HttpRequestRecord> validRecords = records.stream().filter(HttpRequestRecord::isValid).collect(Collectors.toList());
        when(httpSinkClient.send(validRecords)).thenReturn(responses);
        HttpSink httpSink = new HttpSink(httpSinkClient, request, instrumentation);
        OdpfSinkResponse odpfSinkResponse = httpSink.pushToSink(messages);
        Assert.assertTrue(odpfSinkResponse.hasErrors());
        Assert.assertEquals(2, odpfSinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, odpfSinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, odpfSinkResponse.getErrorsFor(2).getErrorType());
    }

    @Test
    public void shouldReportErrors() throws IOException {
        List<OdpfMessage> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, null, true));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));

        Mockito.when(httpRequest.getEntity()).thenReturn(httpEntity);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
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

        HttpSink httpSink = new HttpSink(httpSinkClient, request, instrumentation);
        OdpfSinkResponse odpfSinkResponse = httpSink.pushToSink(messages);
        Assert.assertTrue(odpfSinkResponse.hasErrors());
        Assert.assertEquals(5, odpfSinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, odpfSinkResponse.getErrorsFor(1).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, odpfSinkResponse.getErrorsFor(3).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, odpfSinkResponse.getErrorsFor(4).getErrorType());
    }

    private HttpRequestRecord createRecord(Integer index, ErrorInfo errorInfo, boolean isValid) {
        HttpRequestRecord record = new HttpRequestRecord(errorInfo, isValid, httpRequest);
        record.addIndex(index);
        return record;
    }
}
