package com.gotocompany.depot.http.request.body;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TemplatizedJsonBodyTest {
    @Mock
    private MessageContainer messageContainer;
    @Mock
    private StatsDReporter statsDReporter;
    private Instant time;
    private HttpSinkConfig sinkConfig;
    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        time = Instant.ofEpochSecond(1669160207, 600000000);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestTypesMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoMessageParser protoMessageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
        TestMessage testMessage = TestMessage.newBuilder()
                .setOrderNumber("test-order-1")
                .setOrderDetails("ORDER-DETAILS-1")
                .build();
        TestTypesMessage testTypesMessage = TestTypesMessage.newBuilder()
                .setStringValue("test-string")
                .setFloatValue(10.0f)
                .setBoolValue(true)
                .addListValues("test-list-1").addListValues("test-list-2").addListValues("test-list-3")
                .addListMessageValues(testMessage).addListMessageValues(testMessage)
                .setMessageValue(testMessage)
                .setTimestampValue(timestamp)
                .build();
        Message message = new Message(testMessage.toByteArray(), testTypesMessage.toByteArray());
        ParsedMessage parsedLogMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedLogMessage);
    }

    @Test
    public void shouldReturnJsonBodyWithParameterizedValue() throws IOException {
        configuration.put("SINK_HTTP_JSON_BODY_TEMPLATE",
                "{"
                        + "\"test_float\":\"%s,float_value\", "
                        + "\"%s,string_value\" : "
                        + "{"
                        + "\"xxx\" : \"constant\", "
                        + "\"yyy\" : \"Test-%s-%s,string_value,float_value\" "
                        + "}, "
                        + "\"test_repeated\" : \"%s,list_values\", "
                        + "\"test_repeated_messages\" : \"%s,list_message_values\", "
                        + "\"test_message\" : \"%s,message_value\", "
                        + "\"test_timestamp\" : \"%s,timestamp_value\","
                        + "\"test_seconds\" : \"%s,timestamp_value.seconds\""
                        + "}"
        );
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        RequestBody body = new TemplatizedJsonBody(sinkConfig);
        String stringBody = body.build(messageContainer);
        String expected = "{\"test_timestamp\":\"" + time.toString() + "\",\"test_repeated_messages\":[{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"},{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}],\"test_float\":10,\"test_repeated\":[\"test-list-1\",\"test-list-2\",\"test-list-3\"],\"test-string\":{\"yyy\":\"Test-test-string-10.0\",\"xxx\":\"constant\"},\"test_seconds\":1669160207,\"test_message\":{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}}";
        assertEquals(expected, stringBody);
    }

    @Test
    public void shouldThrowExceptionIfJsonTemplateBodyIsEmpty() {
        configuration.put("SINK_HTTP_JSON_BODY_TEMPLATE", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        ConfigurationException thrown = assertThrows(ConfigurationException.class, () -> new TemplatizedJsonBody(sinkConfig));
        assertEquals("Json body template cannot be empty", thrown.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfJsonTemplateBodyIsNotValid() {
        configuration.put("SINK_HTTP_JSON_BODY_TEMPLATE", "{\"a\"=\"b\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        ConfigurationException thrown = assertThrows(ConfigurationException.class, () -> new TemplatizedJsonBody(sinkConfig));
        assertEquals("Json body template is not a valid json. Expected a ':' after a key at 5 [character 6 line 1]", thrown.getMessage());
    }

    @Test
    public void shouldThrowExceptionForUnknownFieldInTemplate() {
        configuration.put("SINK_HTTP_JSON_BODY_TEMPLATE", "{\"test_string\":\"%s,unknown_field\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        RequestBody body = new TemplatizedJsonBody(sinkConfig);
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> body.build(messageContainer));
        assertEquals("Invalid field config : unknown_field", thrown.getMessage());
    }
}
