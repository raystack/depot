package io.odpf.depot.http.response;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpResponseParser {

    public static Map<Long, ErrorInfo> getErrorsFromResponse(List<HttpRequestRecord> records, List<HttpSinkResponse> responses, Map<Integer, Boolean> retryStatusCodeRanges, Instrumentation instrumentation) throws IOException {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (int i = 0; i < responses.size(); i++) {
            HttpRequestRecord record = records.get(i);
            HttpSinkResponse response = responses.get(i);
            if (response.isFailed()) {
                errors.putAll(getErrors(record, response, retryStatusCodeRanges));
                instrumentation.logError("Error while pushing message request to http services. Record: {}, Response Code: {}, Response Body: {}",
                        record.getRequestBody(), response.getResponseCode(), response.getResponseBody());
            }
        }
        return errors;
    }

    private static Map<Long, ErrorInfo> getErrors(HttpRequestRecord record, HttpSinkResponse response, Map<Integer, Boolean> retryStatusCodeRanges) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        String httpStatusCode = response.getResponseCode();
        for (long messageIndex: record) {
            if (retryStatusCodeRanges.containsKey(Integer.parseInt(httpStatusCode))) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + httpStatusCode), ErrorType.SINK_RETRYABLE_ERROR));
            } else if (httpStatusCode.startsWith("4")) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + httpStatusCode), ErrorType.SINK_4XX_ERROR));
            } else if (httpStatusCode.startsWith("5")) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + httpStatusCode), ErrorType.SINK_5XX_ERROR));
            } else {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + httpStatusCode), ErrorType.SINK_UNKNOWN_ERROR));
            }
        }
        return errors;
    }
}
