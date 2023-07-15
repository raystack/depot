package org.raystack.depot.bigtable.parser;

import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import org.raystack.depot.bigtable.model.BigTableRecord;
import org.raystack.depot.bigtable.response.BigTableResponse;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.metrics.BigTableMetrics;
import org.raystack.depot.metrics.Instrumentation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigTableResponseParser {
    public static Map<Long, ErrorInfo> getErrorsFromSinkResponse(List<BigTableRecord> validRecords,
            BigTableResponse bigTableResponse, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
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
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_UNKNOWN_ERROR));
            }

            instrumentation.logError(
                    "Error while inserting to Bigtable. Record Metadata: {}, Cause: {}, Reason: {}, StatusCode: {}, HttpCode: {}",
                    record.getMetadata(),
                    fm.getError().getCause(),
                    fm.getError().getReason(),
                    fm.getError().getStatusCode().getCode(),
                    fm.getError().getStatusCode().getCode().getHttpStatusCode());

            if (fm.getError().getErrorDetails() == null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String
                        .format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
            } else if (fm.getError().getErrorDetails().getBadRequest() != null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String
                        .format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.BAD_REQUEST));
            } else if (fm.getError().getErrorDetails().getQuotaFailure() != null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String
                        .format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.QUOTA_FAILURE));
            } else if (fm.getError().getErrorDetails().getPreconditionFailure() != null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(
                        BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.PRECONDITION_FAILURE));
            } else {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String
                        .format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
            }
        }
        return errorInfoMap;
    }
}
