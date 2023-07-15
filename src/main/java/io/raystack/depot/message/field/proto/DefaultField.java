package org.raystack.depot.message.field.proto;

import org.raystack.depot.message.field.FieldUtils;
import org.raystack.depot.message.field.GenericField;

public class DefaultField implements GenericField {
    private final Object value;

    public DefaultField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        return FieldUtils.convertToString(value);
    }
}
