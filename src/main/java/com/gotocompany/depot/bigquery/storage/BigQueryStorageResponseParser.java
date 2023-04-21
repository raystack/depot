package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import io.grpc.Status.Code;
import com.google.rpc.Status;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class BigQueryStorageResponseParser {
    private static final Set<Code> RETRYABLE_ERROR_CODES =
            new HashSet<Code>() {{
                add(Code.INTERNAL);
                add(Code.ABORTED);
                add(Code.CANCELLED);
                add(Code.FAILED_PRECONDITION);
                add(Code.DEADLINE_EXCEEDED);
                add(Code.UNAVAILABLE);
            }};
    private final BigQuerySinkConfig sinkConfig;
    private final Instrumentation instrumentation;
    private final BigQueryMetrics bigQueryMetrics;

    public BigQueryStorageResponseParser(
            BigQuerySinkConfig sinkConfig,
            Instrumentation instrumentation,
            BigQueryMetrics bigQueryMetrics) {
        this.sinkConfig = sinkConfig;
        this.instrumentation = instrumentation;
        this.bigQueryMetrics = bigQueryMetrics;
    }

    public static ErrorInfo getError(Status error) {
        com.google.rpc.Code code = com.google.rpc.Code.forNumber(error.getCode());
        switch (code) {
            case OK:
                return null;
            case CANCELLED:
            case INVALID_ARGUMENT:
            case NOT_FOUND:
            case ALREADY_EXISTS:
            case PERMISSION_DENIED:
            case UNAUTHENTICATED:
            case RESOURCE_EXHAUSTED:
            case FAILED_PRECONDITION:
            case ABORTED:
            case OUT_OF_RANGE:
                return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_4XX_ERROR);
            case UNKNOWN:
            case INTERNAL:
            case DATA_LOSS:
            case UNAVAILABLE:
            case UNIMPLEMENTED:
            case UNRECOGNIZED:
            case DEADLINE_EXCEEDED:
                return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_5XX_ERROR);
            default:
                return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_UNKNOWN_ERROR);
        }
    }

    public static boolean shouldRetry(io.grpc.Status status) {
        return BigQueryStorageResponseParser.RETRYABLE_ERROR_CODES.contains(status.getCode());
    }

    public static ErrorInfo get4xxError(RowError rowError) {
        return new ErrorInfo(new Exception(rowError.getMessage()), ErrorType.SINK_4XX_ERROR);
    }

    public static AppendRowsResponse get4xxErrorResponse() {
        return AppendRowsResponse.newBuilder().setError(Status.newBuilder().setCode(com.google.rpc.Code.FAILED_PRECONDITION.ordinal()).build()).build();
    }

    public void setSinkResponseForInvalidMessages(
            BigQueryPayload payload,
            List<Message> messages,
            SinkResponse sinkResponse) {

        payload.forEach(meta -> {
            if (!meta.isValid()) {
                sinkResponse.addErrors(meta.getInputIndex(), meta.getErrorInfo());
                instrumentation.logError(
                        "Error {} occurred while converting to payload for record {}",
                        meta.getErrorInfo(),
                        messages.get((int) meta.getInputIndex()).getMetadataString());
            }
        });
    }

    private void instrumentErrors(Object error) {
        instrumentation.incrementCounter(
                bigQueryMetrics.getBigqueryTotalErrorsMetrics(),
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, sinkConfig.getTableName()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, sinkConfig.getDatasetName()),
                String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, sinkConfig.getGCloudProjectID()),
                String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, error.toString()));
    }

    public void setSinkResponseForErrors(
            BigQueryPayload payload,
            AppendRowsResponse appendRowsResponse,
            List<Message> messages,
            SinkResponse sinkResponse) {
        if (appendRowsResponse.hasError()) {
            instrumentation.logError("received an error in stream :{} ", appendRowsResponse.getError());
            com.google.rpc.Status error = appendRowsResponse.getError();
            ErrorInfo errorInfo = BigQueryStorageResponseParser.getError(error);
            Set<Long> payloadIndexes = payload.getPayloadIndexes();
            com.google.rpc.Code code = com.google.rpc.Code.forNumber(error.getCode());
            payloadIndexes.forEach(index -> {
                long inputIndex = payload.getInputIndex(index);
                sinkResponse.addErrors(inputIndex, errorInfo);
                instrumentErrors(code);
            });
        }

        //per message error
        List<RowError> rowErrorsList = appendRowsResponse.getRowErrorsList();
        rowErrorsList.forEach(rowError -> {
            ErrorInfo errorInfo = BigQueryStorageResponseParser.get4xxError(rowError);
            long inputIndex = payload.getInputIndex(rowError.getIndex());
            sinkResponse.addErrors(inputIndex, errorInfo);
            String metadataString = messages.get((int) inputIndex).getMetadataString();
            instrumentation.logError(
                    "Error {} occurred while sending the payload for record {} with RowError {}",
                    errorInfo,
                    metadataString,
                    rowError);
            instrumentErrors(rowError.getCode());
        });
    }

    public void setSinkResponseForException(
            Throwable cause,
            BigQueryPayload payload,
            List<Message> messages,
            SinkResponse sinkResponse) {
        io.grpc.Status status = io.grpc.Status.fromThrowable(cause);
        instrumentation.logError("Error from exception: {} ", status);
        if (BigQueryStorageResponseParser.shouldRetry(status)) {
            IntStream.range(0, payload.getPayloadIndexes().size())
                    .forEach(index -> {
                        sinkResponse.addErrors(payload.getInputIndex(index), new ErrorInfo(new Exception(cause), ErrorType.SINK_5XX_ERROR));
                        instrumentErrors(status.getCode());
                    });
        } else {
            IntStream.range(0, payload.getPayloadIndexes().size())
                    .forEach(index -> {
                        sinkResponse.addErrors(payload.getInputIndex(index), new ErrorInfo(new Exception(cause), ErrorType.SINK_4XX_ERROR));
                        instrumentErrors(status.getCode());
                    });
        }
        if (cause instanceof Exceptions.AppendSerializationError) {
            Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) cause;
            Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
            rowIndexToErrorMessage.forEach((index, err) -> {
                long inputIndex = payload.getInputIndex(index);
                String metadataString = messages.get((int) inputIndex).getMetadataString();
                ErrorInfo errorInfo = new ErrorInfo(new Exception(err), ErrorType.SINK_4XX_ERROR);
                instrumentation.logError(
                        "Error {} occurred while sending the payload for record {}",
                        errorInfo,
                        metadataString);
                sinkResponse.addErrors(inputIndex, errorInfo);
                instrumentErrors(BigQueryMetrics.BigQueryStorageAPIError.ROW_APPEND_ERROR);
            });
        }
    }
}
