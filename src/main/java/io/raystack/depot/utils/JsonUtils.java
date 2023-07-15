package org.raystack.depot.utils;

import org.raystack.depot.config.RaystackSinkConfig;
import org.json.JSONObject;

public class JsonUtils {
    /**
     * Creates a json Object based on the configuration.
     * If String mode is enabled, it converts all the fields in string.
     *
     * @param config  Sink Configuration
     * @param payload Json Payload in byyes
     * @return Json object
     */
    public static JSONObject getJsonObject(RaystackSinkConfig config, byte[] payload) {
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
