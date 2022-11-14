package io.odpf.depot.http.response;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.metrics.Instrumentation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class HttpResponseParser {

    public static Map<Long, ErrorInfo> parseAndFillError(List<HttpRequestRecord> records, List<HttpSinkResponse> responses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> {
                    HttpSinkResponse response = responses.get(index);
                    if (response.isFailed()) {
                        HttpRequestRecord record = records.get(index);
                        instrumentation.logError("Error while pushing message request to http services. Record: {}, Error: {}",
                                record.getRequestBody(), response.getResponseCode());
                        errors.put(record.getIndex(), new ErrorInfo(new Exception("Error:" + response.getResponseCode()), ErrorType.DEFAULT_ERROR));
                    }
                }
        );
        return errors;
    }
}
