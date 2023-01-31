package io.odpf.depot.http.request;

import io.odpf.depot.TestMessage;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.body.RawBody;
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
    private OdpfMessageParser parser;
    @Mock
    private HttpSinkConfig config;
    private RequestBody requestBody;
    private TestMessage testMessage;

    @Before
    public void setup() {
        testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        requestBody = new RawBody(config);
    }

    @Test
    public void shouldGetValidRequestRecords() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenReturn(new HashMap<>());
        messages.add(new OdpfMessage(null, testMessage.toByteArray()));
        messages.add(new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
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
        messages.add(new OdpfMessage("", 1));
        messages.add(new OdpfMessage(1, "test"));
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
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
        messages.add(new OdpfMessage(null, testMessage.toByteArray()));
        messages.add(new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
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
        messages.add(new OdpfMessage(null, testMessage.toByteArray()));
        messages.add(new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    @Test
    public void shouldGetValidAndInvalidRequestRecords() {
        messages.add(new OdpfMessage(null, ""));
        messages.add(new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(1, invalidRecords.size());
    }
}
