package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.gson.JsonSyntaxException;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.ParsedMessage;

import java.io.IOException;
import java.util.Map;


/**
 * This is a utility class for providing functionality for parsing a templatized Json string
 * using a provided ParsedMessage which contains all the fields of the deserialized Protobuf
 * message. Thus the arguments in the template string will be replaced by the fields from
 * the ParsedMessage
 */
public class JsonParserUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    /**
     * Creates a raw Json Node object from the provided Json template string.
     *
     * @param jsonTemplate the Json template string
     * @return the raw Json node
     */
    public static JsonNode createJsonNode(String jsonTemplate) {
        if (jsonTemplate.isEmpty()) {
            throw new ConfigurationException("Json body template cannot be empty");
        }
        try {
            return OBJECT_MAPPER.readTree(jsonTemplate);

        } catch (JsonSyntaxException | IOException e) {
            throw new ConfigurationException(String.format("Json body template is not a valid json. %s", e.getMessage()));
        }
    }

    /**
     * Parses a raw templatized Json Node using a provided ParsedMessage which contains all
     * the fields of the deserialized Protobuf message. Thus the arguments in the template
     * string will be replaced by the fields from the ParsedMessage Proto object
     *
     * @param rawJsonNode   the raw Json node
     * @param parsedMessage the parsed message
     * @return the parsed Json node
     */
    public static JsonNode parse(JsonNode rawJsonNode, ParsedMessage parsedMessage) {
        switch (rawJsonNode.getNodeType()) {
            case ARRAY:
                return parseInternal((ArrayNode) rawJsonNode, parsedMessage);
            case OBJECT:
                return parseInternal((ObjectNode) rawJsonNode, parsedMessage);
            case STRING:
                return parseInternal((TextNode) rawJsonNode, parsedMessage);
            case NUMBER:
            case BOOLEAN:
            case NULL:
                return rawJsonNode;
            default:
                throw new IllegalArgumentException("The provided Json type is not supported");
        }
    }

    private static JsonNode parseInternal(ObjectNode rawJsonObject, ParsedMessage parsedMessage) {
        ObjectNode parsedJsonObject = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, JsonNode> entry : rawJsonObject.properties()) {
            String rawKeyString = entry.getKey();
            TextNode rawKeyStringNode = new TextNode(rawKeyString);
            JsonNode parsedKeyStringNode = parseInternal(rawKeyStringNode, parsedMessage);
            String parsedKeyString = parsedKeyStringNode.toString();
            if (parsedKeyStringNode.getNodeType().equals(JsonNodeType.STRING)) {
                parsedKeyString = parsedKeyString.substring(1, parsedKeyString.length() - 1);
            }
            JsonNode rawValue = entry.getValue();
            JsonNode parsedValue = parse(rawValue, parsedMessage);
            parsedJsonObject.put(parsedKeyString, parsedValue);
        }
        return parsedJsonObject;
    }

    private static JsonNode parseInternal(ArrayNode rawJsonArray, ParsedMessage parsedMessage) {
        ArrayNode parsedJsonArray = JsonNodeFactory.instance.arrayNode();
        rawJsonArray.forEach(jsonNode -> parsedJsonArray.add(parse(jsonNode, parsedMessage)));
        return parsedJsonArray;
    }

    private static JsonNode parseInternal(TextNode rawJsonStringNode, ParsedMessage parsedMessage) {
        String rawJsonString = rawJsonStringNode.asText();
        if (rawJsonString.isEmpty()) {
            return rawJsonStringNode;
        }
        Template templateValue;
        try {
            templateValue = new Template(rawJsonString);
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        Object parsedValue = templateValue.parseWithType(parsedMessage);
        String parsedJsonString = parsedValue.toString();
        JsonNode parsedJsonNode;
        if (parsedValue instanceof String) {
            if (parsedJsonString.startsWith("\"") && parsedJsonString.endsWith("\"")) {
                parsedJsonString = parsedJsonString.substring(1, parsedJsonString.length() - 1);
            }
            parsedJsonNode = JsonNodeFactory.instance.textNode(parsedJsonString);
            return parsedJsonNode;
        }
        try {
            parsedJsonNode = OBJECT_MAPPER.readTree(parsedJsonString);
        } catch (JsonProcessingException e) {
            throw new ConfigurationException("An error occurred while parsing the template string : " + parsedJsonString + "\nError: " + e.getMessage());
        }
        return parsedJsonNode;
    }
}
