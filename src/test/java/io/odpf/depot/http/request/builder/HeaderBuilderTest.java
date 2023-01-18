package io.odpf.depot.http.request.builder;

import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
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
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "io.odpf.depot.TestBookingLogKey");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTP_HEADERS_PARAMETER_SOURCE", "MESSAGE");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoOdpfMessageParser odpfMessageParser = (ProtoOdpfMessageParser) OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ParsedOdpfMessage parsedOdpfLogMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        ParsedOdpfMessage parsedOdpfLogKey = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());

        when(messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(parsedOdpfLogKey);
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedOdpfLogMessage);
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
