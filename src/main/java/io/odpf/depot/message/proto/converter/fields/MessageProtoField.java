package io.odpf.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MessageProtoField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        return fieldValue;
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE;
    }
}
