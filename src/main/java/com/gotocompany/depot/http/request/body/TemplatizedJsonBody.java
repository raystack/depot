package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;

public class TemplatizedJsonBody implements RequestBody {

    private final JSONObject jsonObject;
    private final HttpSinkConfig config;

    public TemplatizedJsonBody(HttpSinkConfig config) {
        this.config = config;
        this.jsonObject = createJsonObject(config.getSinkHttpJsonBodyTemplate());
    }

    @Override
    public String build(MessageContainer msgContainer) throws IOException {
        ParsedMessage parsedMessage;
        if (config.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_KEY) {
            parsedMessage = msgContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        } else {
            parsedMessage = msgContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        }
        return parse(jsonObject, parsedMessage).toString();
    }

    private JSONObject parse(JSONObject object, ParsedMessage parsedMessage) {
        try {
            Set<String> keys = object.keySet();
            JSONObject finalJsonObject = new JSONObject();
            for (String key : keys) {
                Object value = object.get(key);
                Template templateKey = new Template(key);
                Object parsedKey = templateKey.parseWithType(parsedMessage);
                if (value instanceof JSONObject) {
                    finalJsonObject.put(parsedKey.toString(), parse((JSONObject) value, parsedMessage));
                } else if (value instanceof JSONArray) {
                    JSONArray tempJsonArray = parseJsonArray((JSONArray) value, parsedMessage);
                    finalJsonObject.put(parsedKey.toString(), tempJsonArray);
                } else if (value instanceof String) {
                    Template templateValue = new Template((String) value);
                    finalJsonObject.put(parsedKey.toString(), templateValue.parseWithType(parsedMessage));
                } else {
                    finalJsonObject.put(parsedKey.toString(), value);
                }
            }
            return finalJsonObject;
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private JSONArray parseJsonArray(JSONArray jsonArray, ParsedMessage parsedMessage) throws InvalidTemplateException {
        JSONArray tempJsonArray = new JSONArray();
        for (int i = 0; i < jsonArray.length(); i++) {
            Object object = jsonArray.get(i);
            if (object instanceof JSONObject) {
                tempJsonArray.put(parse((JSONObject) object, parsedMessage));
            } else if (object instanceof JSONArray) {
                tempJsonArray.put(parseJsonArray((JSONArray) object, parsedMessage));
            } else if (object instanceof String) {
                Template templateValue = new Template((String) object);
                tempJsonArray.put(templateValue.parseWithType(parsedMessage));
            } else {
                tempJsonArray.put(object);
            }
        }
        return tempJsonArray;
    }

    private JSONObject createJsonObject(String jsonTemplate) {
        if (jsonTemplate.isEmpty()) {
            throw new ConfigurationException("Json body template cannot be empty");
        }
        try {
            return new JSONObject(jsonTemplate);
        } catch (JSONException e) {
            throw new ConfigurationException(String.format("Json body template is not a valid json. %s", e.getMessage()));
        }
    }
}
