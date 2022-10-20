package io.odpf.depot.http;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.exception.OdpfSinkException;
import io.odpf.depot.http.client.HttpSinkClient;
import io.odpf.depot.http.parser.HttpRequestParser;
import io.odpf.depot.http.parser.HttpResponseParser;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.response.HttpSinkResponse;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpSink implements OdpfSink {

    private final HttpSinkClient httpSinkClient;
    private final HttpRequestParser requestParser;
    private final Instrumentation instrumentation;

    public HttpSink(HttpSinkClient httpSinkClient, HttpRequestParser requestParser, Instrumentation instrumentation) {
        this.httpSinkClient = httpSinkClient;
        this.requestParser = requestParser;
        this.instrumentation = instrumentation;
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException {
        List<HttpRequestRecord> requests = requestParser.convert(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = requests.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);

        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        invalidRecords.forEach(invalidRecord -> odpfSinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (validRecords.size() > 0) {
            List<HttpSinkResponse> responses;
            try {
                responses = httpSinkClient.send(validRecords);
            } catch (IOException e) {
                throw new OdpfSinkException("Exception occurred while execute the request ", e);
            }
            Map<Long, ErrorInfo> errorInfoMap = HttpResponseParser.parseAndFillError(validRecords, responses, instrumentation);
            errorInfoMap.forEach(odpfSinkResponse::addErrors);
            instrumentation.logInfo("Pushed {} records to Http service", validRecords.size());
        }
        return odpfSinkResponse;
    }

    @Override
    public void close() throws IOException {

    }
}
