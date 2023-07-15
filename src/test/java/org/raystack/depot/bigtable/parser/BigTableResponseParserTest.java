package org.raystack.depot.bigtable.parser;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ErrorDetails;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.rpc.BadRequest;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.QuotaFailure;
import org.raystack.depot.TestBookingLogKey;
import org.raystack.depot.TestBookingLogMessage;
import org.raystack.depot.TestServiceType;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.message.Message;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.bigtable.model.BigTableRecord;
import org.raystack.depot.bigtable.response.BigTableResponse;
import org.raystack.depot.metrics.BigTableMetrics;
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
        Mockito.when(apiException.getCause()).thenReturn(apiException);
        Mockito.when(errorDetails.getBadRequest()).thenReturn(null);
        Mockito.when(errorDetails.getQuotaFailure()).thenReturn(null);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(null);

        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1")
                .setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1")
                .setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2")
                .setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2")
                .setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

        Message message1 = new Message(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        Message message2 = new Message(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());

        RowMutationEntry rowMutationEntry1 = RowMutationEntry.create("rowKey1").setCell("family1", "qualifier1",
                "value1");
        RowMutationEntry rowMutationEntry2 = RowMutationEntry.create("rowKey2").setCell("family2", "qualifier2",
                "value2");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry1, 0, null, message1.getMetadata());
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry2, 1, null, message2.getMetadata());
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

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords,
                bigtableResponse, bigtableMetrics, instrumentation);

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

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords,
                bigtableResponse, bigtableMetrics, instrumentation);

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

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords,
                bigtableResponse, bigtableMetrics, instrumentation);

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

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords,
                bigtableResponse, bigtableMetrics, instrumentation);

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

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics,
                instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                bigtableMetrics.getBigtableTotalErrorsMetrics(),
                String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.BAD_REQUEST));
    }

    @Test
    public void shouldCaptureMetricBigtableErrorTypeQuotaFailure() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getQuotaFailure()).thenReturn(QuotaFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics,
                instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                bigtableMetrics.getBigtableTotalErrorsMetrics(),
                String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.QUOTA_FAILURE));
    }

    @Test
    public void shouldCaptureMetricBigtableErrorTypePreconditionFailure() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(PreconditionFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics,
                instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG,
                        BigTableMetrics.BigTableErrorType.PRECONDITION_FAILURE));
    }

    @Test
    public void shouldCaptureMetricBigtableErrorTypeRpcFailureByDefault() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics,
                instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                bigtableMetrics.getBigtableTotalErrorsMetrics(),
                String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
    }

    @Test
    public void shouldCaptureMetricBigtableErrorTypeRpcFailureIfErrorDetailsIsNull() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(apiException.getErrorDetails()).thenReturn(null);
        Mockito.when(code.getHttpStatusCode()).thenReturn(0);

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics,
                instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                bigtableMetrics.getBigtableTotalErrorsMetrics(),
                String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
    }

    @Test
    public void shouldLogErrorRecordWithReasonAndStatusCode() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(PreconditionFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics,
                instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).logError(
                "Error while inserting to Bigtable. Record Metadata: {}, Cause: {}, Reason: {}, StatusCode: {}, HttpCode: {}",
                validRecords.get(1).getMetadata(),
                failedMutations.get(0).getError().getCause(),
                failedMutations.get(0).getError().getReason(),
                failedMutations.get(0).getError().getStatusCode().getCode(),
                failedMutations.get(0).getError().getStatusCode().getCode().getHttpStatusCode());
    }
}
