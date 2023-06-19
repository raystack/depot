package com.gotocompany.depot.http.request.body;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.json.JsonMessageParser;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class JsonBodyTest {

    @Mock
    private MessageContainer messageContainer;
    @Mock
    private StatsDReporter statsDReporter;
    private HttpSinkConfig sinkConfig;
    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        Instant time = Instant.ofEpochSecond(1681303675, 600020000);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestTypesMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestMessage");
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
        Message message = new Message(testMessage.toByteArray(), testTypesMessage.toByteArray(), new Tuple<>("message_topic", "sample-topic"));
        ParsedMessage parsedKey = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        when(messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(parsedKey);
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
        when(messageContainer.getMessage()).thenReturn(message);
    }

    @Test
    public void shouldReturnPayloadWithDateFormatAndMetadata() throws IOException {
        configuration.put("SINK_HTTPV2_DATE_FORMAT_ENABLE", "true");
        configuration.put("SINK_ADD_METADATA_ENABLED", "true");
        configuration.put("SINK_METADATA_COLUMNS_TYPES", "message_topic=string");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        RequestBody body = new JsonBody(sinkConfig);
        String stringBody = body.build(messageContainer);
        String expected = "{\"logKey\":\"{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\",\\\"orderNumber\\\":\\\"test-order-1\\\"}\","
                + "\"topic\":\"sample-topic\","
                + "\"logMessage\":\"{\\\"listValues\\\":[\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"],"
                + "\\\"stringValue\\\":\\\"test-string\\\",\\\"listMessageValues\\\":[{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\","
                + "\\\"orderNumber\\\":\\\"test-order-1\\\"},{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\",\\\"orderNumber\\\":\\\"test-order-1\\\"}],"
                + "\\\"timestampValue\\\":\\\"Apr 12, 2023 12:47:55 PM\\\",\\\"floatValue\\\":10,\\\"boolValue\\\":true,"
                + "\\\"messageValue\\\":{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\",\\\"orderNumber\\\":\\\"test-order-1\\\"}}\"}";
        jsonEquals(expected, stringBody);
    }
    @Test
    public void shouldReturnPayloadWithStringTimestampAndWithoutMetadata() throws IOException {
        configuration.put("SINK_HTTPV2_DATE_FORMAT_ENABLE", "false");
        configuration.put("SINK_ADD_METADATA_ENABLED", "false");
        configuration.put("SINK_METADATA_COLUMNS_TYPES", "message_topic=string");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        RequestBody body = new JsonBody(sinkConfig);
        String stringBody = body.build(messageContainer);
        String expected = "{\"logKey\":\"{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\",\\\"orderNumber\\\":\\\"test-order-1\\\"}\","
                + "\"logMessage\":\"{\\\"listValues\\\":[\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"],"
                + "\\\"stringValue\\\":\\\"test-string\\\",\\\"listMessageValues\\\":[{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\","
                + "\\\"orderNumber\\\":\\\"test-order-1\\\"},{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\",\\\"orderNumber\\\":\\\"test-order-1\\\"}],"
                + "\\\"timestampValue\\\":\\\"2023-04-12T12:47:55.600020Z\\\",\\\"floatValue\\\":10,\\\"boolValue\\\":true,"
                + "\\\"messageValue\\\":{\\\"orderDetails\\\":\\\"ORDER-DETAILS-1\\\",\\\"orderNumber\\\":\\\"test-order-1\\\"}}\"}";
        jsonEquals(expected, stringBody);
    }

    @Test
    public void shouldReturnPayloadFromJsonInput() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", "JSON");
        HttpSinkConfig httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, config);
        JsonMessageParser jsonMessageParser = (JsonMessageParser) MessageParserFactory.getParser(httpSinkConfig, statsDReporter);
        Message jsonMessage = new Message("{\"first_name\": \"john doe\"}".getBytes(), "{\"first_name\": \"john doe\"}".getBytes());
        ParsedMessage jsonParsedMessage = jsonMessageParser.parse(jsonMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "");
        ParsedMessage jsonParsedKey = jsonMessageParser.parse(jsonMessage, SinkConnectorSchemaMessageMode.LOG_KEY, "");
        when(messageContainer.getParsedLogMessage("")).thenReturn(jsonParsedMessage);
        when(messageContainer.getParsedLogKey("")).thenReturn(jsonParsedKey);
        RequestBody body = new JsonBody(httpSinkConfig);
        String stringBody = body.build(messageContainer);
        String expected = "{\"logKey\":\"{\\\"first_name\\\":\\\"john doe\\\"}\",\"logMessage\":\"{\\\"first_name\\\":\\\"john doe\\\"}\"}";
        jsonEquals(expected, stringBody);
    }

    private void jsonEquals(String expected, String actual) {
        assertEquals(new JSONObject(expected).toString(), new JSONObject(actual).toString());
    }
}
