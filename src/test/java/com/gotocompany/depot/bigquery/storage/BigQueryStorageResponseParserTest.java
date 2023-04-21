package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.rpc.Code;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryRecordMeta;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryStorageResponseParserTest {

    private Instrumentation instrumentation;
    private BigQueryStorageResponseParser responseParser;

    @Before
    public void setup() {
        instrumentation = Mockito.mock(Instrumentation.class);
        BigQuerySinkConfig sinkConfig = Mockito.mock(BigQuerySinkConfig.class);
        BigQueryMetrics bigQueryMetrics = new BigQueryMetrics(sinkConfig);
        responseParser = new BigQueryStorageResponseParser(sinkConfig, instrumentation, bigQueryMetrics);
    }

    @Test
    public void shouldReturnErrorFromStatus() {
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder().setCode(Code.PERMISSION_DENIED_VALUE).setMessage("test error").build();
        ErrorInfo error = BigQueryStorageResponseParser.getError(status);
        assert error != null;
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, error.getErrorType());
        Assert.assertEquals("test error", error.getException().getMessage());

        status = com.google.rpc.Status.newBuilder().setCode(Code.INTERNAL_VALUE).setMessage("test 5xx error").build();
        error = BigQueryStorageResponseParser.getError(status);
        assert error != null;
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, error.getErrorType());
        Assert.assertEquals("test 5xx error", error.getException().getMessage());
    }

    @Test
    public void shouldReturnRetryBoolean() {
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.ABORTED));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.INTERNAL));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.CANCELLED));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.FAILED_PRECONDITION));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.DEADLINE_EXCEEDED));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.UNAVAILABLE));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.OK));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.UNKNOWN));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.INVALID_ARGUMENT));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.NOT_FOUND));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.ALREADY_EXISTS));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.PERMISSION_DENIED));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.RESOURCE_EXHAUSTED));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.OUT_OF_RANGE));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.UNIMPLEMENTED));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.DATA_LOSS));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.UNAUTHENTICATED));
    }

    @Test
    public void shouldReturn4xx() {
        RowError rowError = Mockito.mock(RowError.class);
        Mockito.when(rowError.getMessage()).thenReturn("row error");
        ErrorInfo error = BigQueryStorageResponseParser.get4xxError(rowError);
        Assert.assertEquals("row error", error.getException().getMessage());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, error.getErrorType());
    }

    @Test
    public void shouldSetErrorResponse() {
        BigQueryPayload payload = new BigQueryPayload();
        payload.addMetadataRecord(new BigQueryRecordMeta(0, null, true));
        payload.addMetadataRecord(new BigQueryRecordMeta(1, new ErrorInfo(new Exception("error1"), ErrorType.DESERIALIZATION_ERROR), false));
        payload.addMetadataRecord(new BigQueryRecordMeta(2, null, true));
        payload.addMetadataRecord(new BigQueryRecordMeta(3, new ErrorInfo(new Exception("error2"), ErrorType.UNKNOWN_FIELDS_ERROR), false));
        payload.addMetadataRecord(new BigQueryRecordMeta(4, new ErrorInfo(new Exception("error3"), ErrorType.INVALID_MESSAGE_ERROR), false));
        List<Message> messages = createMockMessages();
        Mockito.when(messages.get(1).getMetadataString()).thenReturn("meta1");
        Mockito.when(messages.get(3).getMetadataString()).thenReturn("meta2");
        Mockito.when(messages.get(4).getMetadataString()).thenReturn("meta3");
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForInvalidMessages(payload, messages, response);
        Assert.assertEquals(3, response.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("error1", response.getErrors().get(1L).getException().getMessage());
        Assert.assertEquals("error2", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("error3", response.getErrors().get(4L).getException().getMessage());

        List<BigQueryRecordMeta> metaList = new ArrayList<>();
        payload.forEach(metaList::add);

        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error {} occurred while converting to payload for record {}",
                metaList.get(1).getErrorInfo(), "meta1");
        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error {} occurred while converting to payload for record {}",
                metaList.get(3).getErrorInfo(), "meta2");
        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error {} occurred while converting to payload for record {}",
                metaList.get(4).getErrorInfo(), "meta3");

    }

    @Test
    public void shouldSetResponseForError() {
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 3L);
        payload.putValidIndexToInputIndex(2L, 4L);
        List<Message> messages = createMockMessages();
        AppendRowsResponse appendRowsResponse = AppendRowsResponse.newBuilder().setError(com.google.rpc.Status.newBuilder().setMessage("test error").setCode(Code.UNAVAILABLE_VALUE).build()).build();
        SinkResponse sinkResponse = new SinkResponse();
        responseParser.setSinkResponseForErrors(payload, appendRowsResponse, messages, sinkResponse);
        Assert.assertEquals(3, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(4L).getErrorType());

        Assert.assertEquals("test error", sinkResponse.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("test error", sinkResponse.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("test error", sinkResponse.getErrors().get(4L).getException().getMessage());
    }

    @Test
    public void shouldSetResponseForRowError() {
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 3L);
        payload.putValidIndexToInputIndex(2L, 4L);
        List<Message> messages = createMockMessages();
        AppendRowsResponse appendRowsResponse = AppendRowsResponse.newBuilder()
                .setError(com.google.rpc.Status.newBuilder().setMessage("test error").setCode(Code.UNAVAILABLE_VALUE).build())
                .addRowErrors(RowError.newBuilder().setIndex(1L).setMessage("row error1").build())
                .addRowErrors(RowError.newBuilder().setIndex(2L).setMessage("row error2").build())
                .build();
        SinkResponse sinkResponse = new SinkResponse();
        responseParser.setSinkResponseForErrors(payload, appendRowsResponse, messages, sinkResponse);
        Assert.assertEquals(3, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, sinkResponse.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, sinkResponse.getErrors().get(4L).getErrorType());

        Assert.assertEquals("test error", sinkResponse.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("row error1", sinkResponse.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("row error2", sinkResponse.getErrors().get(4L).getException().getMessage());
    }

    private List<Message> createMockMessages() {
        List<Message> messages = new ArrayList<>();
        Message m1 = Mockito.mock(Message.class);
        Message m2 = Mockito.mock(Message.class);
        Message m3 = Mockito.mock(Message.class);
        Message m4 = Mockito.mock(Message.class);
        Message m5 = Mockito.mock(Message.class);
        messages.add(m1);
        messages.add(m2);
        messages.add(m3);
        messages.add(m4);
        messages.add(m5);
        return messages;
    }

    @Test
    public void shouldSetSinkResponseForException() {
        Throwable cause = new StatusRuntimeException(Status.INTERNAL);
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 1L);
        payload.putValidIndexToInputIndex(2L, 2L);
        payload.putValidIndexToInputIndex(3L, 3L);
        payload.putValidIndexToInputIndex(4L, 4L);
        List<Message> messages = createMockMessages();
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForException(cause, payload, messages, response);
        Assert.assertEquals(5, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(2L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(1L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(2L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(4L).getException().getMessage());
    }

    @Test
    public void shouldSetSinkResponseForExceptionWithNonRetry() {
        Throwable cause = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 1L);
        payload.putValidIndexToInputIndex(2L, 2L);
        payload.putValidIndexToInputIndex(3L, 3L);
        payload.putValidIndexToInputIndex(4L, 4L);
        List<Message> messages = createMockMessages();
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForException(cause, payload, messages, response);
        Assert.assertEquals(5, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(2L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(1L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(2L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(4L).getException().getMessage());
    }

    @Test
    public void shouldSetSinkResponseForExceptionWithAppendError() {
        Map<Integer, String> rowsToErrorMessages = new HashMap<>();
        rowsToErrorMessages.put(0, "message1");
        rowsToErrorMessages.put(2, "message2");
        Throwable cause = new Exceptions.AppendSerializationError(404, "test error", "default", rowsToErrorMessages);
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 3L);
        payload.putValidIndexToInputIndex(2L, 4L);
        List<Message> messages = createMockMessages();
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForException(cause, payload, messages, response);
        Assert.assertEquals(3, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("message1", response.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("com.google.cloud.bigquery.storage.v1.Exceptions$AppendSerializationError: UNKNOWN: test error", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("message2", response.getErrors().get(4L).getException().getMessage());
    }
}
