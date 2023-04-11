package com.gotocompany.depot.schema;

public interface SchemaField {
    String getName();
    SchemaFieldType getType();
    Schema getValueType();
    boolean isRepeated();
}
