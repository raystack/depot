package io.odpf.depot.http.parser;

import io.odpf.depot.TestMessage;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpRequestBodyType;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.enums.HttpRequestType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpRequestParserTest {

    private final List<OdpfMessage> messages = new ArrayList<>();

    @Mock
    private ProtoOdpfMessageParser protoParser;

    @Mock
    private Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    @Mock
    private HttpSinkConfig config;

    @Mock
    private HttpEntityEnclosingRequestBase httpRequest;

    @Before
    public void setup() {
        TestMessage message1 = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        TestMessage message2 = TestMessage.newBuilder().setOrderNumber("test-order-2").setOrderDetails("ORDER-DETAILS-2").build();
        TestMessage message3 = TestMessage.newBuilder().setOrderNumber("test-order-3").setOrderDetails("ORDER-DETAILS-3").build();
        TestMessage message4 = TestMessage.newBuilder().setOrderNumber("test-order-4").setOrderDetails("ORDER-DETAILS-4").build();
        TestMessage message5 = TestMessage.newBuilder().setOrderNumber("test-order-5").setOrderDetails("ORDER-DETAILS-5").build();
        TestMessage message6 = TestMessage.newBuilder().setOrderNumber("test-order-6").setOrderDetails("ORDER-DETAILS-6").build();
        messages.add(new OdpfMessage(null, message1.toByteArray()));
        messages.add(new OdpfMessage(null, message2.toByteArray()));
        messages.add(new OdpfMessage(null, message3.toByteArray()));
        messages.add(new OdpfMessage(null, message4.toByteArray()));
        messages.add(new OdpfMessage(null, message5.toByteArray()));
        messages.add(new OdpfMessage(null, message6.toByteArray()));
    }
    @Test
    public void shouldConvertOdpfMessageToHttpRecordsWithSingleRequest() throws URISyntaxException {
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        when(config.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        when(config.getSinkHttpHeaders()).thenReturn("Accept:text/plain");
        when(config.getSinkHttpServiceUrl()).thenReturn("http://dummy.com");
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
        when(httpRequest.getMethod()).thenReturn("PUT");
        when(httpRequest.getURI()).thenReturn(new URI("http://dummy.com"));
        HttpRequestParser requestParser = new HttpRequestParser(protoParser, modeAndSchema, config);
        List<HttpRequestRecord> parsedRecords = requestParser.convert(messages);

        List<HttpRequestRecord> expectedRecords = new ArrayList<>();
        expectedRecords.add(new HttpRequestRecord(httpRequest, 1L, null, true));
        expectedRecords.add(new HttpRequestRecord(httpRequest, 2L, null, true));
        expectedRecords.add(new HttpRequestRecord(httpRequest, 3L, null, true));
        expectedRecords.add(new HttpRequestRecord(httpRequest, 4L, null, true));
        expectedRecords.add(new HttpRequestRecord(httpRequest, 5L, null, true));
        expectedRecords.add(new HttpRequestRecord(httpRequest, 6L, null, true));
        assertEquals(6, expectedRecords.size());
        for (int index = 0; index < expectedRecords.size(); index++) {
            assertEquals(expectedRecords.get(index).getHttpRequest().getMethod(), parsedRecords.get(index).getHttpRequest().getMethod());
            assertEquals(expectedRecords.get(index).getHttpRequest().getURI(), parsedRecords.get(index).getHttpRequest().getURI());
            assertEquals(expectedRecords.get(index).getHttpRequest().getProtocolVersion(), parsedRecords.get(index).getHttpRequest().getProtocolVersion());
        }
    }

    @Test
    public void shouldConvertOdpfMessageToHttpRecordsWithBatchRequest() {

    }
}
