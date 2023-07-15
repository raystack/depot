package org.raystack.depot.message.field.proto;

import com.google.protobuf.Message;
import org.raystack.depot.message.field.FieldUtils;
import org.raystack.depot.message.field.GenericField;

import java.time.Instant;

public class TimeStampField implements GenericField {
    private final Object value;

    public TimeStampField(Object value) {
        this.value = value;
    }

    public static Instant getInstant(Object field) {
        Message m = (Message) field;
        Object seconds = m.getField(m.getDescriptorForType().findFieldByName("seconds"));
        Object nanos = m.getField(m.getDescriptorForType().findFieldByName("nanos"));
        return Instant.ofEpochSecond((long) seconds, ((Integer) nanos).longValue());
    }

    @Override
    public String getString() {
        return FieldUtils.convertToStringForSpecialTypes(value, Object::toString);
    }
}
