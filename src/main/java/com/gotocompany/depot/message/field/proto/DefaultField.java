package com.gotocompany.depot.message.field.proto;

import com.gotocompany.depot.message.field.FieldUtils;
import com.gotocompany.depot.message.field.GenericField;

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
