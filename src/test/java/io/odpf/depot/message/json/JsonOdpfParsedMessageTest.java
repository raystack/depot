package io.odpf.depot.message.json;

import org.json.JSONObject;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonOdpfParsedMessageTest {
    @Test
    public void shouldGetEmptyMappingKeysForEmptyJsonObject() {
        //for empty json object
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(new JSONObject());
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping(null);
        assertEquals(Collections.emptyMap(), parsedMessageMapping);

    }

    @Test
    public void shouldGetEmptyMappingKeysForNullJsonObject() {
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(null);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping(null);
        assertEquals(Collections.emptyMap(), parsedMessageMapping);
    }

    @Test
    public void shouldGetMappings() {
        JSONObject personDetails = new JSONObject("{\"first_name\": \"john doe\", \"address\": \"planet earth\"}");
        JsonOdpfParsedMessage parsedMessage = new JsonOdpfParsedMessage(personDetails);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping(null);
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("first_name", "john doe");
        expectedMap.put("address", "planet earth");
        assertEquals(expectedMap, parsedMessageMapping);
    }
}
