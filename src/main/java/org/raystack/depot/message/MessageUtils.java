package org.raystack.depot.message;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.json.JSONObject;

import java.io.IOException;

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

    public static void validate(Message message, Class validClass) throws IOException {
        if ((message.getLogKey() != null && !(validClass.isInstance(message.getLogKey())))
                || (message.getLogMessage() != null && !(validClass.isInstance(message.getLogMessage())))) {
            throw new IOException(
                    String.format("Expected class %s, but found: LogKey class: %s, LogMessage class: %s",
                            validClass,
                            message.getLogKey() != null ? message.getLogKey().getClass() : "n/a",
                            message.getLogMessage() != null ? message.getLogMessage().getClass() : "n/a"));
        }
    }
}
