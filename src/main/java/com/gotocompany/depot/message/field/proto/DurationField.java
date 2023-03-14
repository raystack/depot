package com.gotocompany.depot.message.field.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.message.field.FieldUtils;
import com.gotocompany.depot.message.field.GenericField;


public class DurationField implements GenericField {
    private static final double NANO = 1e-9;
    private final Object value;

    public DurationField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        return FieldUtils.convertToStringForSpecialTypes(value, this::getDurationString);
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
