package io.odpf.depot.http;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.exception.OdpfSinkException;
import io.odpf.depot.http.client.HttpSinkClient;
import io.odpf.depot.http.response.HttpResponseParser;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.Request;
import io.odpf.depot.http.response.HttpSinkResponse;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpSink implements OdpfSink {

    private final HttpSinkClient httpSinkClient;
    private final Request request;
    private final Instrumentation instrumentation;
    private final Map<Integer, Boolean> requestLogStatusCodeRanges;

    public HttpSink(HttpSinkClient httpSinkClient, Request request, Instrumentation instrumentation, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        this.httpSinkClient = httpSinkClient;
        this.request = request;
        this.instrumentation = instrumentation;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException {
        List<HttpRequestRecord> requests = request.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = requests.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);

        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        invalidRecords.forEach(invalidRecord -> invalidRecord.forEach(recordIndex -> odpfSinkResponse.addErrors(recordIndex, invalidRecord.getErrorInfo())));
        if (validRecords.size() > 0) {
            instrumentation.logInfo("Processed {} records to Http Service", validRecords.size());
            try {
                List<HttpSinkResponse> responses = httpSinkClient.send(validRecords);
                Map<Long, ErrorInfo> errorInfoMap = HttpResponseParser.getErrorsFromResponse(validRecords, responses, instrumentation, requestLogStatusCodeRanges);
                errorInfoMap.forEach(odpfSinkResponse::addErrors);
            } catch (IOException e) {
                throw new OdpfSinkException("Exception occurred while execute the request ", e);
            }
        }
        return odpfSinkResponse;
    }

    @Override
    public void close() throws IOException {

    }
}
