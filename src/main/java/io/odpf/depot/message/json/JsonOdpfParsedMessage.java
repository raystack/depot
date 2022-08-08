package io.odpf.depot.message.json;

import com.google.gson.Gson;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
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
        return new Gson().fromJson(jsonObject.toString(), HashMap.class);
    }
    public Object getFieldByName(String name, OdpfMessageSchema odpfMessageSchema) {
        String jsonPathName = "$." + name;
        Configuration configuration = Configuration.builder()
                .jsonProvider(new JsonOrgJsonProvider())
                .build();

        JsonPath jsonPath = JsonPath.compile(jsonPathName);
        Object jsonPathString = jsonPath.read(this.getRaw(), configuration);
        if (jsonPathString == null) {
            throw new ConfigurationException("Invalid JsonPath found:" + name);
        }
        return jsonPathString.toString();
    }
}
