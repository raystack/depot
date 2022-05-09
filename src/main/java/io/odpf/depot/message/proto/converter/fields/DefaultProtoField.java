package io.odpf.depot.message.proto.converter.fields;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DefaultProtoField implements ProtoField {
    private final Object fieldValue;

    @Override
    public Object getValue() {
        return fieldValue;
    }

    @Override
    public boolean matches() {
        return false;
    }
}
