package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;

public class ProtoSchemaField implements SchemaField {
    private Descriptors.FieldDescriptor fd;
    public ProtoSchemaField(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fd = fieldDescriptor;
    }

    @Override
    public String getName() {
        return fd.getName();
    }

    @Override
    public String getJsonName() {
        return fd.getJsonName();
    }

    @Override
    public SchemaFieldType getType() {
        switch (fd.getJavaType()) {
            case BYTE_STRING:
                return SchemaFieldType.BYTES;
            default:
                return SchemaFieldType.valueOf(fd.getJavaType().name());
        }
    }

    @Override
    public Schema getValueType() {
        if (fd.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
            return new ProtoSchema(fd.getMessageType());
        }
        return null;
    }

    @Override
    public boolean isRepeated() {
        return fd.isRepeated();
    }
}
