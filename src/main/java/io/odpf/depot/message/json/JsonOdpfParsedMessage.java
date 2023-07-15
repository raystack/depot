package org.raystack.depot.message.json;

import com.jayway.jsonpath.Configuration;
import org.raystack.depot.config.OdpfSinkConfig;
import org.raystack.depot.message.MessageUtils;
import org.raystack.depot.message.OdpfMessageSchema;
import org.raystack.depot.message.ParsedOdpfMessage;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Map;

public class JsonOdpfParsedMessage implements ParsedOdpfMessage {
    private final JSONObject jsonObject;
    private final Configuration jsonPathConfig;

    public JsonOdpfParsedMessage(JSONObject jsonObject, Configuration jsonPathConfig) {
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
    public void validate(OdpfSinkConfig config) {

    }

    @Override
    public Map<String, Object> getMapping(OdpfMessageSchema schema) {
        if (jsonObject == null || jsonObject.isEmpty()) {
            return Collections.emptyMap();
        }
        return jsonObject.toMap();
    }

    public Object getFieldByName(String name, OdpfMessageSchema raystackMessageSchema) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, jsonObject, jsonPathConfig);
    }
}
