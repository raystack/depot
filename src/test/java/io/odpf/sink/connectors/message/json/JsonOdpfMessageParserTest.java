package io.odpf.sink.connectors.message.json;

import io.odpf.sink.connectors.expcetion.EmptyMessageException;
import io.odpf.sink.connectors.message.InputSchemaMessageMode;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class JsonOdpfMessageParserTest {

    /*
        JSONObject.equals does reference check, so cant use assertEquals instead we use expectedJson.similar(actualJson)
        reference https://github.com/stleary/JSON-java/blob/master/src/test/java/org/json/junit/JSONObjectTest.java#L132
     */
    @Test
    public void shouldParseJsonLogMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String validJsonStr = "{\"first_name\":\"john\"}";
        JsonOdpfMessage jsonOdpfMessage = new JsonOdpfMessage(null, validJsonStr.getBytes());

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    @Test
    public void shouldThrowErrorForInvalidLogMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String invalidJsonStr = "{\"first_";
        JsonOdpfMessage jsonOdpfMessage = new JsonOdpfMessage(null, invalidJsonStr.getBytes());
        IOException ioException = assertThrows(IOException.class, () -> {
            jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_MESSAGE, null);
        });
        assertEquals("invalid json error", ioException.getMessage());
        assertTrue(ioException.getCause() instanceof JSONException);
    }

    @Test
    public void shouldThrowEmptyMessageException() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        JsonOdpfMessage jsonOdpfMessage = new JsonOdpfMessage(null, null);
        EmptyMessageException emptyMessageException = assertThrows(EmptyMessageException.class, () -> {
            jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_MESSAGE, null);
        });
        assertEquals("log message is empty", emptyMessageException.getMessage());
    }

    @Test
    public void shouldParseJsonKeyMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String validJsonStr = "{\"first_name\":\"john\"}";
        JsonOdpfMessage jsonOdpfMessage = new JsonOdpfMessage(validJsonStr.getBytes(), null);

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_KEY, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    @Test
    public void shouldThrowErrorForInvalidKeyMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String invalidJsonStr = "{\"first_";
        JsonOdpfMessage jsonOdpfMessage = new JsonOdpfMessage(invalidJsonStr.getBytes(), null);
        IOException ioException = assertThrows(IOException.class, () -> {
            jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_KEY, null);
        });
        assertEquals("invalid json error", ioException.getMessage());
        assertTrue(ioException.getCause() instanceof JSONException);
    }

    @Test
    public void shouldThrowErrorWhenModeNotDefined() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String invalidJsonStr = "{\"first_";
        JsonOdpfMessage jsonOdpfMessage = new JsonOdpfMessage(invalidJsonStr.getBytes(), null);
        IOException ioException = assertThrows(IOException.class, () -> {
            jsonOdpfMessageParser.parse(jsonOdpfMessage, null, null);
        });
        assertEquals("message mode not defined", ioException.getMessage());
    }
}
