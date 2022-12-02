package io.odpf.depot.message.field.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.depot.message.field.GenericField;

import java.util.Collection;
import java.util.stream.Collectors;

public class DurationField implements GenericField {
    private static final double NANO = 1e-9;
    private final Object value;

    public DurationField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        if (value instanceof Collection<?>) {
            return "[" + ((Collection<?>) value).stream().map(this::getDurationString).collect(Collectors.joining(",")) + "]";
        }
        return getDurationString(value);
    }

    private String getDurationString(Object field) {
        DynamicMessage message = (DynamicMessage) field;
        Descriptors.FieldDescriptor secondsDescriptor = message.getDescriptorForType().findFieldByName("seconds");
        Descriptors.FieldDescriptor nanosDescriptor = message.getDescriptorForType().findFieldByName("nanos");
        long seconds = (Long) message.getField(secondsDescriptor);
        int nanos = (Integer) message.getField(nanosDescriptor);
        return (seconds + (nanos * NANO)) + "s";
    }
}
