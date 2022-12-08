package io.odpf.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

public class DurationProtoField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    public DurationProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    @Override
    public Object getValue() {
        return fieldValue;
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && descriptor.getMessageType().getFullName().equals(com.google.protobuf.Duration.getDescriptor().getFullName());
    }
}
