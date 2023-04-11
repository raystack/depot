package com.gotocompany.depot.message.json;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.json.GenericJsonSchema;
import com.gotocompany.depot.schema.json.GenericJsonSchemaField;
import com.jayway.jsonpath.Configuration;
import org.json.JSONObject;

import java.util.Map;
import java.util.stream.Collectors;

public class JsonParsedMessage implements ParsedMessage {
    private final JSONObject jsonObject;
    private final Configuration jsonPathConfig;
    private final Schema schema;

    public JsonParsedMessage(JSONObject jsonObject, Configuration jsonPathConfig) {
        this.jsonObject = jsonObject;
        this.jsonPathConfig = jsonPathConfig;
        this.schema = new GenericJsonSchema(jsonObject);
    }

    public String toString() {
        return jsonObject.toString();
    }

    @Override
    public Object getRaw() {
        return jsonObject;
    }

    @Override
    public void validate(SinkConfig config) { }

    @Override
    public Map<SchemaField, Object> getFields() {
        return jsonObject.keySet().stream().collect(Collectors.toMap(s -> new GenericJsonSchemaField(s, jsonObject.get(s)), jsonObject::get));
    }

    public Object getFieldByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, jsonObject, jsonPathConfig);
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public LogicalValue getLogicalValue() {
        return null;
    }
}
