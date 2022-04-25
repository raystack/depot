package io.odpf.sink.connectors.message.json;

import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.message.OdpfMessageSchema;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;
import org.json.JSONObject;

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
        return null;
    }
}
