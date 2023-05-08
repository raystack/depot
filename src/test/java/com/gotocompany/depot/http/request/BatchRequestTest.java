package com.gotocompany.depot.http.request;

import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.request.builder.HeaderBuilder;
import com.gotocompany.depot.http.request.builder.QueryParamBuilder;
import com.gotocompany.depot.http.request.builder.UriBuilder;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BatchRequestTest {

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
    private TestMessage testMessage;

    @Before
    public void setup() {
        testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
    }

    @Test
    public void shouldWrapMessagesToSingleRequestBody() throws IOException {
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
        assertEquals("[{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}, {\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}]", parsedRecords.get(0).getRequestBody());
    }

    @Test
    public void shouldGetValidRequestRecords() {
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
    }

    @Test
    public void shouldGetInvalidRequestRecords() {
        messages.add(new Message("", 1));
        messages.add(new Message(1, "test"));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
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
        messages.add(new Message("", 1));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(1, invalidRecords.size());
    }

    @Test
    public void shouldCreateDeleteRequestWithBody() throws IOException, InvalidTemplateException {
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.DELETE);
        when(config.isSinkHttpDeleteBodyEnable()).thenReturn(true);
        when(config.getSinkHttpServiceUrl()).thenReturn("localhost:8080/value123");
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        UriBuilder uriBuilder1 = new UriBuilder(config);
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder1, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
        assertEquals("\n"
                + "Request Method: DELETE\n"
                + "Request Url: localhost:8080/value123\n"
                + "Request Headers: []\n"
                + "Request Body: [{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}, {\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}]",
                parsedRecords.get(0).getRequestString());
    }

    @Test
    public void shouldCreateDeleteRequestWithoutBody() throws IOException, InvalidTemplateException {
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.DELETE);
        when(config.isSinkHttpDeleteBodyEnable()).thenReturn(false);
        when(config.getSinkHttpServiceUrl()).thenReturn("localhost:8080/value123");
        UriBuilder uriBuilder1 = new UriBuilder(config);
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder1, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
        assertEquals("\n"
                + "Request Method: DELETE\n"
                + "Request Url: localhost:8080/value123\n"
                + "Request Headers: []", parsedRecords.get(0).getRequestString());
    }
}
