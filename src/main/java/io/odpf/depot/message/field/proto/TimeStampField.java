package io.odpf.depot.message.field.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.odpf.depot.message.field.GenericField;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TimeStampField implements GenericField {
    private final Object value;

    public TimeStampField(Object value) {
        this.value = value;

    }

    @Override
    public String getString() {
        Message dynamicField = (Message) value;
        List<Descriptors.FieldDescriptor> descriptors = dynamicField.getDescriptorForType().getFields();
        List<Object> timeFields = new ArrayList<>();
        descriptors.forEach(desc -> timeFields.add(dynamicField.getField(desc)));
        return Instant.ofEpochSecond((long) timeFields.get(0), ((Integer) timeFields.get(1)).longValue()).toString();
    }
}
