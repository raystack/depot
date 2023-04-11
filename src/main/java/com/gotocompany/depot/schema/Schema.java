package com.gotocompany.depot.schema;

import java.util.List;

public interface Schema {
    String getFullName();
    List<SchemaField> getFields();
    SchemaField getFieldByName(String name);

    LogicalType logicalType();
}
