package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import org.json.JSONObject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Schema interface implementation from JSONObject.
 * This implementation derives schema from JSONObject by type checking the JSON values.
 */
public class GenericJsonSchema implements Schema {
    private final JSONObject json;

    public GenericJsonSchema(JSONObject json) {
        this.json = json;
    }

    @Override
    public String getFullName() {
        return "root";
    }

    @Override
    public List<SchemaField> getFields() {
        return json.keySet().stream().map(s -> new GenericJsonSchemaField(s, json.get(s))).collect(Collectors.toList());
    }

    @Override
    public SchemaField getFieldByName(String name) {
        Object value = json.get(name);
        return new GenericJsonSchemaField(name, value);
    }

    @Override
    public LogicalType logicalType() {
        return LogicalType.MESSAGE;
    }
}
