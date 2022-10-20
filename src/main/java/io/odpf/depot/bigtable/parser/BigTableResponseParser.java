package io.odpf.depot.bigtable.parser;

import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.metrics.BigTableMetrics;
import io.odpf.depot.metrics.Instrumentation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigTableResponseParser {
    public static Map<Long, ErrorInfo> parseAndFillOdpfSinkResponse(List<BigTableRecord> validRecords, BigTableResponse bigTableResponse, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        HashMap<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        for (MutateRowsException.FailedMutation fm : bigTableResponse.getFailedMutations()) {
            BigTableRecord record = validRecords.get(fm.getIndex());
            long messageIndex = record.getIndex();

            String httpStatusCode = String.valueOf(fm.getError().getStatusCode().getCode().getHttpStatusCode());
            if (fm.getError().isRetryable()) {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_RETRYABLE_ERROR));
            } else if (httpStatusCode.startsWith("4")) {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_4XX_ERROR));
            } else if (httpStatusCode.startsWith("5")) {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_5XX_ERROR));
            } else {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.DEFAULT_ERROR));
            }
            instrumentation.logError("Error while inserting to Bigtable. Record: {}, Error: {}, Reason: {}", record.toString(), fm.getError(), fm.getError().getReason());
            instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, fm.getError().getReason()));
        }
        return errorInfoMap;
    }
}
