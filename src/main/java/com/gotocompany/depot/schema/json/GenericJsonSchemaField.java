package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;

public class GenericJsonSchemaField implements SchemaField {
    private final String name;

    private final SchemaFieldType type;

    private final boolean isRepeated;
    private final Object value;

    public GenericJsonSchemaField(String fieldName, Object value) {
        this.name = fieldName;
        this.type = getFieldType(value);
        this.value = value;
        this.isRepeated = value instanceof JSONArray;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getJsonName() {
        return name;
    }

    private SchemaFieldType getFieldType(Object inputValue) {
        if (inputValue instanceof JSONObject) {
            return SchemaFieldType.MESSAGE;
        } else if (inputValue instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) inputValue;
            return jsonArray.isEmpty() ? SchemaFieldType.STRING : getFieldType(jsonArray.get(0));
        } else if (inputValue instanceof BigDecimal) {
            return SchemaFieldType.DOUBLE;
        } else if (inputValue instanceof Integer) {
            return SchemaFieldType.INT;
        } else if (inputValue instanceof Float) {
            return SchemaFieldType.FLOAT;
        } else if (inputValue instanceof Long) {
            return SchemaFieldType.LONG;
        } else if (inputValue == Boolean.TRUE || inputValue == Boolean.FALSE) {
            return SchemaFieldType.BOOLEAN;
        }
        return SchemaFieldType.STRING;
    }

    @Override
    public SchemaFieldType getType() {
        return type;
    }

    @Override
    public Schema getValueType() {
        return new GenericJsonSchema((JSONObject) value);
    }

    @Override
    public boolean isRepeated() {
        return isRepeated;
    }
}
