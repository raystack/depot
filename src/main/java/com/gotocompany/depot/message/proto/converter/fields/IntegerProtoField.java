package com.gotocompany.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

import java.util.Collection;
import java.util.stream.Collectors;

public class IntegerProtoField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    public IntegerProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    @Override
    public Object getValue() {
        if (fieldValue instanceof Collection<?>) {
            return ((Collection<?>) fieldValue).stream().map(this::getValue).collect(Collectors.toList());
        }
        return getValue(fieldValue);
    }

    public Long getValue(Object field) {
        return Long.valueOf(field.toString());
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.INT64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.UINT64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.FIXED64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SFIXED64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SINT64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.INT32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.UINT32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.FIXED32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SFIXED32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SINT32;
    }
}
