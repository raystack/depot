package io.odpf.depot.http.response;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.metrics.Instrumentation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpResponseParser {

    public static Map<Long, ErrorInfo> getErrorsFromResponse(List<HttpRequestRecord> records, List<HttpSinkResponse> responses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (int index = 0; index < responses.size(); index++) {
            HttpRequestRecord record = records.get(index);
            HttpSinkResponse response = responses.get(index);
            List<Integer> messageIndex = record.getIndex();
            String httpStatusCode = response.getResponseCode();
            if (responses.get(index).isFailed()) {
                if (httpStatusCode.startsWith("4")) {
                    messageIndex.forEach(recordIndex -> errors.put(Long.valueOf(recordIndex), new ErrorInfo(new Exception("Error:" + httpStatusCode), ErrorType.SINK_4XX_ERROR)));
                } else if (httpStatusCode.startsWith("5")) {
                    messageIndex.forEach(recordIndex -> errors.put(Long.valueOf(recordIndex), new ErrorInfo(new Exception("Error:" + httpStatusCode), ErrorType.SINK_5XX_ERROR)));
                } else {
                    messageIndex.forEach(recordIndex -> errors.put(Long.valueOf(recordIndex), new ErrorInfo(new Exception("Error:" + httpStatusCode), ErrorType.SINK_UNKNOWN_ERROR)));
                }
            }
            instrumentation.logError("Error while pushing message request to http services. Record: {}, Error: {}", record.getRequestBody(), response.getResponseCode());
        }
        return errors;
    }
}
