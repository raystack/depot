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
    private MessageContainer messageContainer;
    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestBookingLogKey");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTPV2_QUERY_PARAMETER_SOURCE", "MESSAGE");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoMessageParser messageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ParsedMessage parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        ParsedMessage parsedLogKey = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());

        when(messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(parsedLogKey);
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
    }

    @Test
    public void shouldGenerateConstantQueryParameter() {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"order_number\":\"V-1234\", \"order_details\":\"test-details\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build();

        assertEquals("V-1234", queryParam.get("order_number"));
        assertEquals("test-details", queryParam.get("order_details"));
    }

    @Test
    public void shouldGenerateQueryParameterFromTemplate() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(1, queryParam.size());
        assertEquals("V-GO_SEND", queryParam.get("H-ON#1"));
    }

    @Test
    public void shouldGenerateQueryParameterFromTemplateWhenQueryParamSourceIsKey() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_url\":\"V-%s,order_number\"}");
        configuration.put("SINK_HTTPV2_QUERY_PARAMETER_SOURCE", "KEY");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(1, queryParam.size());
        assertEquals("V-ON#1", queryParam.get("H-OURL#1"));
    }

    @Test
    public void shouldHandleConstantStringInTemplateAlongWithParameterizedQuery() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\", \"H-const\":\"V-const\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(2, queryParam.size());
        assertEquals("V-GO_SEND", queryParam.get("H-ON#1"));
        assertEquals("V-const", queryParam.get("H-const"));
    }

    @Test
    public void shouldReturnEmptyCollectionIfQueryTemplateIsEmpty() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    @Test
    public void shouldReturnEmptyMapIfQueryTemplateIsEmptyString() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    @Test
    public void shouldReturnEmptyMapIfQueryTemplateIsNotProvided() throws IOException {
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfAnyFieldNameProvidedDoesNotExistInSchema() {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,RANDOM_FIELD\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);

        try {
            queryParamBuilder.build(messageContainer);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Invalid field config : RANDOM_FIELD", e.getMessage());
        }
    }
}
