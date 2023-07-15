package org.raystack.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class TimestampProtoField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        if (fieldValue instanceof Collection<?>) {
            return ((Collection<?>) fieldValue).stream().map(this::getTime).collect(Collectors.toList());
        }
        return getTime(fieldValue);
    }

    private Instant getTime(Object field) {
        DynamicMessage dynamicField = (DynamicMessage) field;
        List<Descriptors.FieldDescriptor> descriptors = dynamicField.getDescriptorForType().getFields();
        List<Object> timeFields = new ArrayList<>();
        descriptors.forEach(desc -> timeFields.add(dynamicField.getField(desc)));
        return Instant.ofEpochSecond((long) timeFields.get(0), ((Integer) timeFields.get(1)).longValue());
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && descriptor.getMessageType().getFullName()
                        .equals(com.google.protobuf.Timestamp.getDescriptor().getFullName());
    }
}
