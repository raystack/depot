package io.odpf.depot.message.field.proto;

import io.odpf.depot.message.field.GenericField;

import java.util.Collection;
import java.util.stream.Collectors;

public class StructField implements GenericField {
    private final Object value;

    public StructField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        if (value instanceof Collection<?>) {
            Object messageJsons = ((Collection<?>) value)
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            return "[" + messageJsons + "]";
        } else {
            return value.toString();
        }
    }
}
