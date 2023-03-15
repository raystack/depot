package com.gotocompany.depot.message.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonParsedMessageTest {
    private final Configuration configuration = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    @Test
    public void shouldGetEmptyMappingKeysForEmptyJsonObject() {
        //for empty json object
        JsonParsedMessage parsedMessage = new JsonParsedMessage(new JSONObject(), configuration);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping();
        assertEquals(Collections.emptyMap(), parsedMessageMapping);

    }

    @Test
    public void shouldGetEmptyMappingKeysForNullJsonObject() {
        JsonParsedMessage parsedMessage = new JsonParsedMessage(null, configuration);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping();
        assertEquals(Collections.emptyMap(), parsedMessageMapping);
    }

    @Test
    public void shouldGetMappings() {
        JSONObject personDetails = new JSONObject("{\"first_name\": \"john doe\", \"address\": \"planet earth\"}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        Map<String, Object> parsedMessageMapping = parsedMessage.getMapping();
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("first_name", "john doe");
        expectedMap.put("address", "planet earth");
        assertEquals(expectedMap, parsedMessageMapping);
    }

    @Test
    public void shouldReturnValueFromFlatJson() {
        JSONObject personDetails = new JSONObject("{\"first_name\": \"john doe\", \"address\": \"planet earth\"}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        assertEquals("john doe", parsedMessage.getFieldByName("first_name"));
    }

    @Test
    public void shouldReturnValueFromNestedJson() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : {\"brother\" : \"david doe\"}"
                + "}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        assertEquals("david doe", parsedMessage.getFieldByName("family.brother"));
    }

    @Test
    public void shouldThrowExceptionIfNotFound() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : {\"brother\" : \"david doe\"}"
                + "}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        java.lang.IllegalArgumentException illegalArgumentException = Assert.assertThrows(java.lang.IllegalArgumentException.class, () -> parsedMessage.getFieldByName("family.sister"));
        Assert.assertEquals("Invalid field config : family.sister", illegalArgumentException.getMessage());
    }

    @Test
    public void shouldReturnListFromNestedJson() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : [{\"brother\" : \"david doe\"}, {\"brother\" : \"cain doe\"}]"
                + "}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        JSONArray family = (JSONArray) parsedMessage.getFieldByName("family");
        Assert.assertEquals(2, family.length());
        Assert.assertEquals("david doe", ((JSONObject) family.get(0)).get("brother"));
        Assert.assertEquals("cain doe", ((JSONObject) family.get(1)).get("brother"));
    }
}
