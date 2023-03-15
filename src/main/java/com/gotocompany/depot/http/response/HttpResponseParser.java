package com.gotocompany.depot.http.response;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpResponseParser {

    public static Map<Long, ErrorInfo> getErrorsFromResponse(
            List<HttpRequestRecord> records,
            List<HttpSinkResponse> responses,
            Map<Integer, Boolean> retryStatusCodeRanges,
            Map<Integer, Boolean> requestLogStatusCodeRanges,
            Instrumentation instrumentation) throws IOException {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (int i = 0; i < responses.size(); i++) {
            HttpRequestRecord record = records.get(i);
            HttpSinkResponse response = responses.get(i);
            String responseCode = response.getResponseCode();
            if (shouldLogRequest(responseCode, requestLogStatusCodeRanges)) {
                instrumentation.logInfo(record.getRequestString());
            }
            if (response.isFailed()) {
                errors.putAll(getErrors(record, responseCode, retryStatusCodeRanges));
                instrumentation.logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", responseCode, response.getResponseBody());
            }
        }
        return errors;
    }

    private static Map<Long, ErrorInfo> getErrors(HttpRequestRecord record, String responseCode, Map<Integer, Boolean> retryStatusCodeRanges) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (long messageIndex: record) {
            if (retryStatusCodeRanges.containsKey(Integer.parseInt(responseCode))) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_RETRYABLE_ERROR));
            } else if (responseCode.startsWith("4")) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_4XX_ERROR));
            } else if (responseCode.startsWith("5")) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_5XX_ERROR));
            } else {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_UNKNOWN_ERROR));
            }
        }
        return errors;
    }

    private static boolean shouldLogRequest(String responseCode, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        return responseCode.equals("null") || requestLogStatusCodeRanges.containsKey(Integer.parseInt(responseCode));
    }
}
