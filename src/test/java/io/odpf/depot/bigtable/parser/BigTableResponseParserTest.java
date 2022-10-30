package io.odpf.depot.bigtable.parser;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ErrorDetails;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.rpc.BadRequest;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.QuotaFailure;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.metrics.BigTableMetrics;
import io.odpf.depot.metrics.Instrumentation;
import org.aeonbits.owner.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BigTableResponseParserTest {
    @Mock
    private BigTableMetrics bigtableMetrics;
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private ApiException apiException;
    @Mock
    private StatusCode statusCode;
    @Mock
    private StatusCode.Code code;
    @Mock
    private ErrorDetails errorDetails;
    private List<BigTableRecord> validRecords;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(apiException.getStatusCode()).thenReturn(statusCode);
        Mockito.when(statusCode.getCode()).thenReturn(code);
        Mockito.when(apiException.getErrorDetails()).thenReturn(errorDetails);
        Mockito.when(apiException.getReason()).thenReturn("REASON_STRING");
        Mockito.when(apiException.isRetryable()).thenReturn(Boolean.FALSE);
        Mockito.when(errorDetails.getBadRequest()).thenReturn(null);
        Mockito.when(errorDetails.getQuotaFailure()).thenReturn(null);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(null);

        RowMutationEntry rowMutationEntry1 = RowMutationEntry.create("rowKey1").setCell("family1", "qualifier1", "value1");
        RowMutationEntry rowMutationEntry2 = RowMutationEntry.create("rowKey2").setCell("family2", "qualifier2", "value2");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry1, 0, null, true);
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry2, 1, null, true);
        validRecords = Collections.list(bigTableRecord1, bigTableRecord2);
    }

    @Test
    public void shouldReturnErrorInfoMapWithRetryableError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(400);
        Mockito.when(apiException.isRetryable()).thenReturn(Boolean.TRUE);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    @Test
    public void shouldReturnErrorInfoMapWith4XXError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(400);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_4XX_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    @Test
    public void shouldReturnErrorInfoMapWith5XXError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(500);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_5XX_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    @Test
    public void shouldReturnErrorInfoMapWithUnknownError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_UNKNOWN_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    @Test
    public void shouldCaptureMetricBigtableErrorTypeBadRequest() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getBadRequest()).thenReturn(BadRequest.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.BAD_REQUEST));
    }

    @Test
    public void shouldCaptureMetricBigtableErrorTypeQuotaFailure() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getQuotaFailure()).thenReturn(QuotaFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.QUOTA_FAILURE));
    }

    @Test
    public void shouldCaptureMetricBigtableErrorTypePreconditionFailure() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(PreconditionFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.PRECONDITION_FAILURE));
    }

    @Test
    public void shouldLogErrorRecordWithReasonAndStatusCode() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(PreconditionFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error while inserting to Bigtable. Record: {}, Error: {}, Reason: {}, StatusCode: {}, HttpCode: {}",
                validRecords.get(1).toString(),
                failedMutations.get(0).getError(),
                failedMutations.get(0).getError().getReason(),
                failedMutations.get(0).getError().getStatusCode().getCode(),
                failedMutations.get(0).getError().getStatusCode().getCode().getHttpStatusCode());
    }
}
