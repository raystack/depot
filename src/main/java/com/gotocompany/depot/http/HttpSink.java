package com.gotocompany.depot.http;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.http.client.HttpSinkClient;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.request.Request;
import com.gotocompany.depot.http.response.HttpResponseParser;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpSink implements Sink {

    private final HttpSinkClient httpSinkClient;
    private final Request request;
    private final Map<Integer, Boolean> retryStatusCodeRanges;
    private final Instrumentation instrumentation;
    private final Map<Integer, Boolean> requestLogStatusCodeRanges;

    public HttpSink(HttpSinkClient httpSinkClient, Request request, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges, Instrumentation instrumentation) {
        this.httpSinkClient = httpSinkClient;
        this.request = request;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.instrumentation = instrumentation;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        List<HttpRequestRecord> requests = request.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = requests.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);

        SinkResponse sinkResponse = new SinkResponse();
        invalidRecords.forEach(invalidRecord -> invalidRecord.forEach(recordIndex -> sinkResponse.addErrors(recordIndex, invalidRecord.getErrorInfo())));
        if (validRecords.size() > 0) {
            instrumentation.logInfo("Processed {} records to Http Service", validRecords.size());
            try {
                List<HttpSinkResponse> responses = httpSinkClient.send(validRecords);
                Map<Long, ErrorInfo> errorInfoMap = HttpResponseParser.getErrorsFromResponse(validRecords, responses, retryStatusCodeRanges, requestLogStatusCodeRanges, instrumentation);
                errorInfoMap.forEach(sinkResponse::addErrors);
            } catch (IOException e) {
                throw new SinkException("Exception occurred while execute the request ", e);
            }
        }
        return sinkResponse;
    }

    @Override
    public void close() throws IOException {

    }
}
