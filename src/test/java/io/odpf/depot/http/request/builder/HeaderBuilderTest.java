package io.odpf.depot.http.request.builder;

import com.google.protobuf.Descriptors;
import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestLocation;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.parsers.Template;
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

public class HeaderBuilderTest {

    @Mock
    private HttpSinkConfig config;
    @Mock
    private StatsDReporter statsDReporter;
    private final Map<String, String> configuration = new HashMap<>();
    private ProtoOdpfMessageParser odpfMessageParser;
    private Map<Template, Template> headersTemplate;
    private OdpfMessageSchema headersParameterSourceSchema;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTP_HEADERS_PARAMETER_SOURCE", "MESSAGE");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        headersTemplate = new HashMap<>();
        odpfMessageParser = (ProtoOdpfMessageParser) OdpfMessageParserFactory.getParser(config, statsDReporter);
        Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestBookingLogKey.class.getName()), TestBookingLogKey.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestServiceType.class.getName()), TestServiceType.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
        }};
        headersParameterSourceSchema = odpfMessageParser.getSchema(config.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);
    }

    @Test
    public void shouldGenerateBaseHeader() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        assertEquals("json", headerBuilder.build().get("content-type"));
    }

    @Test
    public void shouldHandleMultipleHeader() {
        configuration.put("SINK_HTTP_HEADERS", "Authorization:auth_token,Accept:text/plain");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        Map<String, String> header = headerBuilder.build();
        assertEquals("auth_token", header.get("Authorization"));
        assertEquals("text/plain", header.get("Accept"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        configuration.put("SINK_HTTP_HEADERS", "");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        headerBuilder.build();
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowErrorIfHeaderConfigIsInvalid() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json,header_key;header_value,key:,:value");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        headerBuilder.build();
    }

    @Test
    public void shouldGenerateParameterisedHeaderFromTemplateWhenModeEqualsLogMessage() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        headersTemplate.put(new Template("H-%s,order_number"), new Template("V-%s,service_type"));
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());

        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        Map<String, String> headers = headerBuilder.build(message);

        assertEquals(2, headers.size());
        assertEquals("json", headers.get("content-type"));
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
    }

    @Test
    public void shouldGenerateParameterisedHeaderFromTemplateWhenModeEqualsLogMessageAndBaseHeadersAreNotProvided() throws IOException {
        headersTemplate.put(new Template("H-%s,order_number"), new Template("V-%s,service_type"));
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());

        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        Map<String, String> headers = headerBuilder.build(message);

        assertEquals(1, headers.size());
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
    }

    @Test
    public void shouldHandleConstantHeaderStringsProvidedInTemplateAlongWithAnyFormattedString() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        headersTemplate.put(new Template("H-1"), new Template("V-%s,service_type"));
        headersTemplate.put(new Template("H-2"), new Template("V-2"));
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());

        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        Map<String, String> headers = headerBuilder.build(message);

        assertEquals(3, headers.size());
        assertEquals("V-GO_SEND", headers.get("H-1"));
        assertEquals("V-2", headers.get("H-2"));
        assertEquals("json", headers.get("content-type"));
    }

    @Test
    public void shouldReturnBaseHeadersIfHeaderTemplateIsNull() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());

        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), null, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        Map<String, String> headers = headerBuilder.build(message);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    @Test
    public void shouldReturnBaseHeadersIfHeaderTemplateIsEmpty() throws IOException {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());

        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), new HashMap<>(), SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        Map<String, String> headers = headerBuilder.build(message);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfAnyFieldNameProvidedDoesNotExistInSchema() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        headersTemplate.put(new Template("H-1"), new Template("V-%s,RANDOM_FIELD"));
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());

        HeaderBuilder headerBuilder = new HeaderBuilder(odpfMessageParser, config.getSinkHttpHeaders(), headersTemplate, SinkConnectorSchemaMessageMode.LOG_MESSAGE, config.getSinkConnectorSchemaProtoMessageClass(), headersParameterSourceSchema);

        try {
            headerBuilder.build(message);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Invalid field config : RANDOM_FIELD", e.getMessage());
        }
    }
}
