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
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
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

@RunWith(MockitoJUnitRunner.class)
public class BatchRequestTest {

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
    public void shouldWrapMessagesToSingleRequestBody() throws IOException {
        messages.add(new OdpfMessage(null, testMessage.toByteArray()));
        messages.add(new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
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
        messages.add(new OdpfMessage(null, testMessage.toByteArray()));
        messages.add(new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
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
        messages.add(new OdpfMessage("", 1));
        messages.add(new OdpfMessage(1, "test"));
        Request requestParser = new BatchRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
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
        messages.add(new OdpfMessage("", 1));
        messages.add(new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(HttpRequestMethodType.PUT, headerBuilder, queryParamBuilder, uriBuilder, requestBody, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(1, invalidRecords.size());
    }
}
