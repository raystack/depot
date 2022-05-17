package io.odpf.depot.message.json;

import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Map;

public class JsonOdpfParsedMessage implements ParsedOdpfMessage {
    private final JSONObject jsonObject;

    public JsonOdpfParsedMessage(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
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
            return  Collections.emptyMap();
        }
        return jsonObject.toMap();
    }
}
