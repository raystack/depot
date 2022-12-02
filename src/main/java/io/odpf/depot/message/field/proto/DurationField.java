package io.odpf.depot.message.field.proto;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.odpf.depot.message.field.GenericField;

import java.util.Collection;
import java.util.stream.Collectors;

public class DurationField implements GenericField {
    private static final double NANO = 1e-9;
    private static final Gson GSON = new GsonBuilder().create();
    private final Object value;

    public DurationField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        if (value instanceof Collection<?>) {
            return GSON.toJson(((Collection<?>) value).stream().map(this::getDurationString).collect(Collectors.toList()));
        }
        return getDurationString(value);
    }

    private String getDurationString(Object field) {
        Message message = (Message) field;
        Descriptors.FieldDescriptor secondsDescriptor = message.getDescriptorForType().findFieldByName("seconds");
        Descriptors.FieldDescriptor nanosDescriptor = message.getDescriptorForType().findFieldByName("nanos");
        long seconds = (Long) message.getField(secondsDescriptor);
        int nanos = (Integer) message.getField(nanosDescriptor);
        if (nanos != 0) {
            return (seconds + (nanos * NANO)) + "s";
        } else {
            return seconds + "s";
        }
    }
}
