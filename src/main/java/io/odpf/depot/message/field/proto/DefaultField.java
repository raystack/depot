package io.odpf.depot.message.field.proto;

import io.odpf.depot.message.field.FieldUtils;
import io.odpf.depot.message.field.GenericField;

public class DefaultField implements GenericField {
    private final Object value;

    public DefaultField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        return FieldUtils.convertToStringWithGSON(value, Object::toString);
    }
}
