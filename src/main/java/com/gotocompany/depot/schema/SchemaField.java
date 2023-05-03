package com.gotocompany.depot.schema;

public interface SchemaField {
    String getName();
    String getJsonName();
    SchemaFieldType getType();
    Schema getValueType();
    boolean isRepeated();
}
