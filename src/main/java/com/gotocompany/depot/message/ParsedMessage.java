package com.gotocompany.depot.message;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;

import java.util.Map;

public interface ParsedMessage {
    Object getRaw();

    void validate(SinkConfig config);

    Map<SchemaField, Object> getFields();

    Object getFieldByName(String name);

    Schema getSchema();

    LogicalValue getLogicalValue();
}
