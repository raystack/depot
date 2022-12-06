package io.odpf.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

public class MapProtoField implements ProtoField {

    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    public MapProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    @Override
    public Object getValue() {
        return fieldValue;
    }

    @Override
    public boolean matches() {
        return descriptor.isMapField();
    }
}
