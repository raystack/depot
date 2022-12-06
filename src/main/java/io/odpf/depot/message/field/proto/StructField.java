package io.odpf.depot.message.field.proto;

import io.odpf.depot.message.field.FieldUtils;
import io.odpf.depot.message.field.GenericField;


public class StructField implements GenericField {
    private final Object value;

    public StructField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        // This Struct is already converted into strings, so we just need to concat it.
        return FieldUtils.convertToStringForMessageTypes(value, Object::toString);
    }
}
