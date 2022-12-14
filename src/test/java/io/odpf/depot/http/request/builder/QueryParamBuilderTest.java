package io.odpf.depot.http.request.builder;

import com.google.protobuf.Descriptors;
import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestLocation;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.OdpfMessageSchema;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class QueryParamBuilderTest {

    private HttpSinkConfig sinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private ProtoOdpfMessageParser parser;
    @Mock
    private MessageContainer messageContainer;

    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "io.odpf.depot.TestBookingLogKey");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTP_QUERY_PARAMETER_SOURCE", "MESSAGE");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoOdpfMessageParser odpfMessageParser = (ProtoOdpfMessageParser) OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);

        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ParsedOdpfMessage parsedOdpfLogMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        ParsedOdpfMessage parsedOdpfLogKey = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());

        Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestBookingLogKey.class.getName()), TestBookingLogKey.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.TopicMetadata.class.getName()), TestBookingLogMessage.TopicMetadata.getDescriptor());
            put(String.format("%s", TestServiceType.class.getName()), TestServiceType.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
        }};
        OdpfMessageSchema messageSchema = odpfMessageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);
        OdpfMessageSchema keySchema = odpfMessageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoKeyClass(), descriptorsMap);

        when(parser.getSchema(sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(keySchema);
        when(parser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(messageSchema);
        when(messageContainer.getParsedLogKey(parser, sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(parsedOdpfLogKey);
        when(messageContainer.getParsedLogMessage(parser, sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedOdpfLogMessage);
    }

    @Test
    public void shouldGenerateConstantQueryParameter() {
        configuration.put("SINK_HTTP_QUERY_TEMPLATE", "{\"order_number\":\"V-1234\", \"order_details\":\"test-details\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build();

        assertEquals("V-1234", queryParam.get("order_number"));
        assertEquals("test-details", queryParam.get("order_details"));
    }

    @Test
    public void shouldGenerateQueryParameterFromTemplate() throws IOException {
        configuration.put("SINK_HTTP_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer, parser);

        assertEquals(1, queryParam.size());
        assertEquals("V-GO_SEND", queryParam.get("H-ON#1"));
    }

    @Test
    public void shouldGenerateQueryParameterFromTemplateWhenQueryParamSourceIsKey() throws IOException {
        configuration.put("SINK_HTTP_QUERY_TEMPLATE", "{\"H-%s,order_url\":\"V-%s,order_number\"}");
        configuration.put("SINK_HTTP_QUERY_PARAMETER_SOURCE", "KEY");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer, parser);

        assertEquals(1, queryParam.size());
        assertEquals("V-ON#1", queryParam.get("H-OURL#1"));
    }

    @Test
    public void shouldHandleConstantStringInTemplateAlongWithParameterizedQuery() throws IOException {
        configuration.put("SINK_HTTP_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\", \"H-const\":\"V-const\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer, parser);

        assertEquals(2, queryParam.size());
        assertEquals("V-GO_SEND", queryParam.get("H-ON#1"));
        assertEquals("V-const", queryParam.get("H-const"));
    }

    @Test
    public void shouldReturnEmptyCollectionIfQueryTemplateIsEmpty() throws IOException {
        configuration.put("SINK_HTTP_QUERY_TEMPLATE", "{}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer, parser);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    @Test
    public void shouldReturnEmptyMapIfQueryTemplateIsEmptyString() throws IOException {
        configuration.put("SINK_HTTP_QUERY_TEMPLATE", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer, parser);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    @Test
    public void shouldReturnEmptyMapIfQueryTemplateIsNotProvided() throws IOException {
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer, parser);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfAnyFieldNameProvidedDoesNotExistInSchema() {
        configuration.put("SINK_HTTP_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,RANDOM_FIELD\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);

        try {
            queryParamBuilder.build(messageContainer, parser);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Invalid field config : RANDOM_FIELD", e.getMessage());
        }
    }
}
