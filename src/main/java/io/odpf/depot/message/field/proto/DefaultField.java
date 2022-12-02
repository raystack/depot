package io.odpf.depot.message.field.proto;

import com.google.protobuf.DynamicMessage;
import io.odpf.depot.message.field.GenericField;

import java.util.Collection;
import java.util.stream.Collectors;

public class DefaultField implements GenericField {
    private final Object value;

    public DefaultField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        if (value instanceof Collection<?>) {
            return "[" + ((Collection<?>) value).stream().map(ob -> {
                if (ob instanceof DynamicMessage) {
                    return new MessageField(ob).getString();
                } else {
                    return new DefaultField(ob).getString();
                }
            }).collect(Collectors.joining(",")) + "]";
        }
        return value.toString();
    }
}
