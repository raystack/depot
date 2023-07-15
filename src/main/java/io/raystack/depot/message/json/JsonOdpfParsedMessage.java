package org.raystack.depot.message.json;

import com.jayway.jsonpath.Configuration;
import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.depot.message.MessageUtils;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Map;

public class JsonRaystackParsedMessage implements ParsedRaystackMessage {
    private final JSONObject jsonObject;
    private final Configuration jsonPathConfig;

    public JsonRaystackParsedMessage(JSONObject jsonObject, Configuration jsonPathConfig) {
        this.jsonObject = jsonObject;
        this.jsonPathConfig = jsonPathConfig;
    }

    public String toString() {
        return jsonObject.toString();
    }

    @Override
    public Object getRaw() {
        return jsonObject;
    }

    @Override
    public void validate(RaystackSinkConfig config) {

    }

    @Override
    public Map<String, Object> getMapping(RaystackMessageSchema schema) {
        if (jsonObject == null || jsonObject.isEmpty()) {
            return Collections.emptyMap();
        }
        return jsonObject.toMap();
    }

    public Object getFieldByName(String name, RaystackMessageSchema raystackMessageSchema) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, jsonObject, jsonPathConfig);
    }
}
