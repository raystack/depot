package io.odpf.depot.message;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.json.JSONObject;

public class MessageUtils {

    public static Object getFieldFromJsonObject(String name, JSONObject jsonObject, Configuration jsonPathConfig) {
        try {
            String jsonPathName = "$." + name;
            JsonPath jsonPath = JsonPath.compile(jsonPathName);
            return jsonPath.read(jsonObject, jsonPathConfig);
        } catch (PathNotFoundException e) {
            throw new IllegalArgumentException("Invalid field config : " + name, e);
        }
    }
}
