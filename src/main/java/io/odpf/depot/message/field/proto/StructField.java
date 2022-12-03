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
        return FieldUtils.convertToString(value, Object::toString);
    }
}
