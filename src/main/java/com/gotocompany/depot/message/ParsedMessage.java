package com.gotocompany.depot.message;

import com.gotocompany.depot.config.SinkConfig;
import org.json.JSONObject;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;

import java.util.Map;

public interface ParsedMessage {
    Object getRaw();

    JSONObject toJson();

    void validate(SinkConfig config);

    Map<SchemaField, Object> getFields();

    Object getFieldByName(String name);

    Schema getSchema();

    LogicalValue getLogicalValue();
}
