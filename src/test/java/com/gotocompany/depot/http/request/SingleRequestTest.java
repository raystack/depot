package com.gotocompany.depot.http.request;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.http.request.body.RawBody;
import com.gotocompany.depot.http.request.body.RequestBody;
import com.gotocompany.depot.http.request.builder.HeaderBuilder;
import com.gotocompany.depot.http.request.builder.QueryParamBuilder;
import com.gotocompany.depot.http.request.builder.UriBuilder;
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

    private final List<Message> messages = new ArrayList<>();
    @Mock
    private HeaderBuilder headerBuilder;
    @Mock
    private QueryParamBuilder queryParamBuilder;
    @Mock
    private UriBuilder uriBuilder;
    @Mock
    private MessageParser parser;
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
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
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
        messages.add(new Message("", 123));
        messages.add(new Message(123, "test"));
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
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
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
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
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
        messages.add(new Message(null, ""));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
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
