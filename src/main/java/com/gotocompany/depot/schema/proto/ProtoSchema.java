package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ProtoSchema implements Schema {
    private final Descriptors.Descriptor descriptor;
    private Map<String, ProtoSchemaField> fields;

    public ProtoSchema(Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public String getFullName() {
        return descriptor.getFullName();
    }

    @Override
    public List<SchemaField> getFields() {
        initializeFields();
        return new ArrayList<>(fields.values());
    }

    @Override
    public SchemaField getFieldByName(String name) {
        initializeFields();
        return fields.get(name);
    }

    @Override
    public LogicalType logicalType() {
        String fullName = descriptor.getFullName();
        if (Timestamp.getDescriptor().getFullName().equals(fullName)) {
            return LogicalType.TIMESTAMP;
        } else if (Duration.getDescriptor().getFullName().equals(fullName)) {
            return LogicalType.DURATION;
        } else if (Struct.getDescriptor().getFullName().equals(fullName)) {
            return LogicalType.STRUCT;
        } else if (descriptor.getOptions().getMapEntry()) {
            return LogicalType.MAP;
        }
        return LogicalType.MESSAGE;
    }

    private void initializeFields() {
        if (fields == null) {
            fields = descriptor.getFields().stream()
                    .map(ProtoSchemaField::new)
                    .collect(Collectors.toMap(ProtoSchemaField::getName, Function.identity()));
        }
    }
}
