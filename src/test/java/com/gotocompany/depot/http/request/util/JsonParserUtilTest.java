package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonParserUtilTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @Mock
    private StatsDReporter statsDReporter;
    private Instant time;
    private HttpSinkConfig sinkConfig;
    private ParsedMessage parsedLogMessage;
    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        time = Instant.ofEpochSecond(1669160207, 600000000);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestTypesMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE", "false");
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
                .setInt32Value(445)
                .setInt64Value(299283773722L)
                .setBoolValue(true)
                .addListValues("test-list-1").addListValues("test-list-2").addListValues("test-list-3")
                .addListMessageValues(testMessage).addListMessageValues(testMessage)
                .setMessageValue(testMessage)
                .setTimestampValue(timestamp)
                .build();
        Message message = new Message(testMessage.toByteArray(), testTypesMessage.toByteArray());
        parsedLogMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestTypesMessage");
    }

    @Test
    public void shouldParseJsonFloatType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("23.6677");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("23.6677", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonArrayTypeWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"ss\",23]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"ss\",23]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"sss\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"sss\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonIntegerType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("234");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("234", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonLongType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("23492992920");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("23492992920", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonObjectTypeWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"aa\":22,\"gg\":true,\"ss\":\"ee\",\"pp\":33.45}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"aa\":22,\"gg\":true,\"ss\":\"ee\",\"pp\":33.45}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonBooleanType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("false");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("false", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithArrayArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"test-list-1\",\"test-list-2\",\"test-list-3\"]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,message_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithNestedStringArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,message_value.order_number\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"test-order-1\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithFloatArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,float_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("10.0", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithTimestampArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,timestamp_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"2022-11-22T23:36:47.600Z\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,bool_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("true", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithIntegerArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("445", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithLongArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,int64_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("299283773722", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithStringArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,string_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"test-string\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",bool_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"true\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithIntegerArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"445\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithLongArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",int64_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"299283773722\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithObjectArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",message_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"{\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"}\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithArrayArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"[\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithFloatArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",float_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"10.0\"", parsedJsonNode.toString());
    }


    @Test
    public void shouldParseJsonStringTemplateWithArrayArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"array = %s,list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"array = [\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithObjectArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"object = %s,message_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"object = {\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"}\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithFloatArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"float = %s,float_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"float = 10.0\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"bool = %s,bool_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"bool = true\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithIntegerArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"int32 = %s,int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"int32 = 445\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithLongArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"int64 = %s,int64_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"int64 = 299283773722\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithMultipleTypeArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"int64 = %s object = %s bool = %s array = %s,int64_value,message_value,bool_value,list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"int64 = 299283773722 object = {\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"} bool = true array = [\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\"", parsedJsonNode.toString());
    }


    @Test
    public void shouldParseJsonStringTemplateWithStringArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"string = %s,string_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"string = test-string\"", parsedJsonNode.toString());
    }


    @Test
    public void shouldParseJsonObjectTemplateWithPrimitiveTypeArgumentsInValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"%s,bool_value\",\"hh\":\"%s,float_value\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":true,\"hh\":10.0}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonObjectTemplateWithArrayTypeArgumentsInValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"%s,list_values\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":[\"test-list-1\",\"test-list-2\",\"test-list-3\"]}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonObjectTemplateWithObjectTypeArgumentsInValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"%s,message_value\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonObjectTemplateWithPrimitiveTypeArgumentsInKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"%s,bool_value\":\"ss\",\"%s,float_value\":\"hh\",\"%s,string_value\":\"vv\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"true\":\"ss\",\"10.0\":\"hh\",\"test-string\":\"vv\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonObjectTemplateWithArrayTypeArgumentsInKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"%s,list_values\":\"ss\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"[\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\":\"ss\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonObjectTemplateWithObjectTypeArgumentsInKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"%s,message_value\":\"ss\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"{\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"}\":\"ss\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonNullStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("null");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("null", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonArrayTemplateWithPrimitiveArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"%s,string_value\",\"%s,float_value\",\"%s,bool_value\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"test-string\",10.0,true]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonArrayTemplateWithArrayArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"%s,list_values\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[[\"test-list-1\",\"test-list-2\",\"test-list-3\"]]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonArrayTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"%s,message_value\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseEmptyJsonArray() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseEmptyJsonObject() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseEmptyJsonStringInObjectValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":\"\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseEmptyJsonStringInObjectKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"\":\"ss\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"\":\"ss\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseEmptyJsonStringInArray() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"ss\",\"\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"ss\",\"\"]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseEmptyJsonString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"\"", parsedJsonNode.toString());
    }
}
