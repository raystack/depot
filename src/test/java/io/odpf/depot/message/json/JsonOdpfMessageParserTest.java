package io.odpf.depot.message.json;

import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.expcetion.EmptyMessageException;
import io.odpf.depot.message.InputSchemaMessageMode;
import io.odpf.depot.message.OdpfMessage;
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
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, validJsonStr.getBytes());

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    @Test
    public void shouldThrowErrorForInvalidLogMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String invalidJsonStr = "{\"first_";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, invalidJsonStr.getBytes());
        IOException ioException = assertThrows(IOException.class, () -> {
            jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_MESSAGE, null);
        });
        assertEquals("invalid json error", ioException.getMessage());
        assertTrue(ioException.getCause() instanceof JSONException);
    }

    @Test
    public void shouldThrowEmptyMessageException() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        OdpfMessage jsonOdpfMessage = new OdpfMessage(null, null);
        EmptyMessageException emptyMessageException = assertThrows(EmptyMessageException.class, () -> {
            jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_MESSAGE, null);
        });
        assertEquals("log message is empty", emptyMessageException.getMessage());
    }

    @Test
    public void shouldParseJsonKeyMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String validJsonStr = "{\"first_name\":\"john\"}";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(validJsonStr.getBytes(), null);

        ParsedOdpfMessage parsedOdpfMessage = jsonOdpfMessageParser.parse(jsonOdpfMessage, InputSchemaMessageMode.LOG_KEY, null);
        JSONObject actualJson = (JSONObject) parsedOdpfMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    @Test
    public void shouldThrowErrorForInvalidKeyMessage() throws IOException {
        JsonOdpfMessageParser jsonOdpfMessageParser = new JsonOdpfMessageParser(null);
        String invalidJsonStr = "{\"first_";
        OdpfMessage jsonOdpfMessage = new OdpfMessage(invalidJsonStr.getBytes(), null);
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
        OdpfMessage jsonOdpfMessage = new OdpfMessage(invalidJsonStr.getBytes(), null);
        IOException ioException = assertThrows(IOException.class, () -> {
            jsonOdpfMessageParser.parse(jsonOdpfMessage, null, null);
        });
        assertEquals("message mode not defined", ioException.getMessage());
    }
}
