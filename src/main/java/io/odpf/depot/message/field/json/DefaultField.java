package io.odpf.depot.message.field.json;

import io.odpf.depot.message.field.GenericField;

public class DefaultField implements GenericField {
    private final Object value;

    public DefaultField(Object field) {
        this.value = field;
    }

    @Override
    public String getString() {
        return value.toString();
    }
}
