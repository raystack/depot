package io.odpf.depot.bigquery.client;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import io.odpf.depot.bigquery.error.ErrorDescriptor;
import io.odpf.depot.bigquery.error.ErrorParser;
import io.odpf.depot.bigquery.error.InvalidSchemaError;
import io.odpf.depot.bigquery.error.OOBError;
import io.odpf.depot.bigquery.error.StoppedError;
import io.odpf.depot.bigquery.error.UnknownError;
import io.odpf.depot.bigquery.exception.BigQuerySinkException;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.metrics.BigQueryMetrics;
import io.odpf.depot.metrics.Instrumentation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryResponseParser {
    /**
     * Parses the {@link InsertAllResponse} object and returns errors type {@link ErrorDescriptor}.
     * {@link InsertAllResponse} in bqResponse are 1 to 1 indexed based on the records that are requested to be inserted.
     *
     * @param records    - list of records that were tried with BQ insertion
     * @param bqResponse - the status of insertion for all records as returned by BQ
     * @return list of messages with error.
     */
    public static Map<Long, ErrorInfo> getErrorsFromBQResponse(
            final List<Record> records,
            final InsertAllResponse bqResponse,
            BigQueryMetrics bigQueryMetrics,
            Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errorInfoResponse = new HashMap<>();
        if (!bqResponse.hasErrors()) {
            return errorInfoResponse;
        }
        Map<Long, List<BigQueryError>> insertErrorsMap = bqResponse.getInsertErrors();
        for (final Map.Entry<Long, List<BigQueryError>> errorEntry : insertErrorsMap.entrySet()) {
            Record record = records.get(errorEntry.getKey().intValue());
            long messageIndex = record.getIndex();
            List<ErrorDescriptor> errors = ErrorParser.parseError(errorEntry.getValue());
            instrumentation.logError("Error while bigquery insert for message. Record: {}, Error: {}, MetaData: {}",
                    record.getColumns(), errors, record.getMetadata());

            if (errorMatch(errors, UnknownError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.UNKNOWN_ERROR));
            } else if (errorMatch(errors, InvalidSchemaError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.INVALID_SCHEMA_ERROR));
            } else if (errorMatch(errors, OOBError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.OOB_ERROR));
            } else if (errorMatch(errors, StoppedError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_5XX_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.STOPPED_ERROR));
            }
        }
        return errorInfoResponse;
    }

    private static boolean errorMatch(List<ErrorDescriptor> errors, Class c) {
        return errors.stream().anyMatch(errorDescriptor -> errorDescriptor.getClass().equals(c));
    }
}
