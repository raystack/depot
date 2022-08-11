package io.odpf.depot.utils;

import io.odpf.depot.config.OdpfSinkConfig;
import org.json.JSONObject;

public class JsonUtils {
    public static JSONObject getJsonObject(OdpfSinkConfig config, byte[] payload) {
        JSONObject jsonObject = new JSONObject(new String(payload));
        if (!config.getSinkConnectorSchemaJsonParserStringModeEnabled()) {
            return jsonObject;
        }
        // convert to all objects to string
        JSONObject jsonWithStringValues = new JSONObject();
        jsonObject.keySet()
                .forEach(k -> {
                    Object value = jsonObject.get(k);
                    if (value instanceof JSONObject) {
                        throw new UnsupportedOperationException("nested json structure not supported yet");
                    }
                    if (JSONObject.NULL.equals(value)) {
                        return;
                    }
                    jsonWithStringValues.put(k, value.toString());
                });

        return jsonWithStringValues;
    }
}
