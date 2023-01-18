package io.odpf.depot.http.request;

import io.odpf.depot.TestMessage;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.QueryParamBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SingleRequestTest {

    private final List<OdpfMessage> messages = new ArrayList<>();
    @Mock
    private HeaderBuilder headerBuilder;
    @Mock
    private QueryParamBuilder queryParamBuilder;
    @Mock
    private UriBuilder uriBuilder;
    @Mock
    private RequestBody requestBody;
    @Mock
    private OdpfMessageParser odpfMessageParser;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        TestMessage message = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        messages.add(new OdpfMessage(null, message.toByteArray()));
        messages.add(new OdpfMessage(message.toByteArray(), message.toByteArray()));
    }

    @Test
    public void shouldGetValidRequestRecords() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenReturn(new HashMap<>());
        when(requestBody.build(messages.get(0))).thenReturn("{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}");
        when(requestBody.build(messages.get(1))).thenReturn("{\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}");
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, odpfMessageParser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(2, validRecords.size());
        assertEquals(0, invalidRecords.size());
    }

    @Test
    public void shouldGetInvalidRequestRecords() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenReturn(new HashMap<>());
        when(requestBody.build(messages.get(0))).thenThrow(IOException.class);
        when(requestBody.build(messages.get(1))).thenThrow(DeserializerException.class);
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, odpfMessageParser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    @Test
    public void shouldGetInvalidRequestRecordsWhenHeaderBuilderThrowsIOException() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenThrow(IOException.class);
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, odpfMessageParser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    @Test
    public void shouldGetInvalidRequestRecordsWhenHeaderBuilderThrowsIllegalArgumentException() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenThrow(IOException.class);
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, odpfMessageParser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    @Test
    public void shouldGetValidAndInvalidRequestRecords() throws IOException {
        when(requestBody.build(messages.get(0))).thenThrow(IOException.class);
        when(requestBody.build(messages.get(1))).thenReturn("{\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}");
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, odpfMessageParser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(1, invalidRecords.size());
    }
}
