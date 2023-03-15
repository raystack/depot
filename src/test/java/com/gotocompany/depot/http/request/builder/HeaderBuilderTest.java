package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestServiceType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class HeaderBuilderTest {

    private HttpSinkConfig sinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private MessageContainer messageContainer;
    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestBookingLogKey");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTP_HEADERS_PARAMETER_SOURCE", "MESSAGE");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoMessageParser protoMessageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        ParsedMessage parsedLogKey = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());

        when(messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(parsedLogKey);
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
    }

    @Test
    public void shouldGenerateBaseHeader() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);

        assertEquals("json", headerBuilder.build().get("content-type"));
    }

    @Test
    public void shouldHandleMultipleBaseHeaders() {
        configuration.put("SINK_HTTP_HEADERS", "Authorization:auth_token,Accept:text/plain");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> header = headerBuilder.build();

        assertEquals("auth_token", header.get("Authorization"));
        assertEquals("text/plain", header.get("Accept"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        configuration.put("SINK_HTTP_HEADERS", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        headerBuilder.build();
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowErrorIfHeaderConfigIsInvalid() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json,header_key;header_value,key:,:value");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        headerBuilder.build();
    }

    @Test
    public void shouldGenerateParameterisedHeaderFromTemplate() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        configuration.put("SINK_HTTP_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(2, headers.size());
        assertEquals("json", headers.get("content-type"));
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
    }

    @Test
    public void shouldGenerateParameterisedHeaderFromTemplateWhenHeaderParamSourceIsKey() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        configuration.put("SINK_HTTP_HEADERS_TEMPLATE", "{\"H-%s,order_url\":\"V-%s,order_number\"}");
        configuration.put("SINK_HTTP_HEADERS_PARAMETER_SOURCE", "KEY");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(2, headers.size());
        assertEquals("json", headers.get("content-type"));
        assertEquals("V-ON#1", headers.get("H-OURL#1"));
    }

    @Test
    public void shouldGenerateParameterisedHeaderFromTemplateWhenBaseHeadersAreNotProvided() throws IOException {
        configuration.put("SINK_HTTP_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
    }

    @Test
    public void shouldHandleConstantHeaderStringsProvidedInTemplateAlongWithAnyFormattedString() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        configuration.put("SINK_HTTP_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\", \"H-const\":\"V-const\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(3, headers.size());
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
        assertEquals("V-const", headers.get("H-const"));
        assertEquals("json", headers.get("content-type"));
    }

    @Test
    public void shouldReturnBaseHeadersIfHeadersTemplateIsEmpty() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        configuration.put("SINK_HTTP_HEADERS_TEMPLATE", "{}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    @Test
    public void shouldReturnBaseHeadersIfHeadersTemplateIsEmptyString() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        configuration.put("SINK_HTTP_HEADERS_TEMPLATE", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    @Test
    public void shouldReturnBaseHeadersIfHeadersTemplateIsNotProvided() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfAnyFieldNameProvidedDoesNotExistInSchema() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        configuration.put("SINK_HTTP_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,RANDOM_FIELD\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);

        try {
            headerBuilder.build(messageContainer);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Invalid field config : RANDOM_FIELD", e.getMessage());
        }
    }
}
