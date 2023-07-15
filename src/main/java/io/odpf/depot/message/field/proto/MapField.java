package org.raystack.depot.message.field.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.raystack.depot.message.field.GenericField;
import org.json.JSONObject;

import java.util.Collection;

public class MapField implements GenericField {
    private final Object value;

    public MapField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        JSONObject json = new JSONObject();
        ((Collection<?>) value).forEach(m -> {
            Message mapEntry = (Message) m;
            String k = mapEntry.getField(mapEntry.getDescriptorForType().findFieldByName("key")).toString();
            Descriptors.FieldDescriptor vDescriptor = mapEntry.getDescriptorForType().findFieldByName("value");
            Object v = mapEntry.getField(vDescriptor);
            if (vDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                    && vDescriptor.getMessageType().getFullName()
                            .equals(com.google.protobuf.Timestamp.getDescriptor().getFullName())) {
                json.put(k, new TimeStampField(TimeStampField.getInstant(v)).getString());
            } else if (vDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                    && vDescriptor.getMessageType().getFullName()
                            .equals(com.google.protobuf.Duration.getDescriptor().getFullName())) {
                json.put(k, new DurationField(v).getString());
            } else if (vDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                json.put(k, new JSONObject(new MessageField(v).getString()));
            } else {
                json.put(k, new DefaultField(v).getString());
            }
        });
        return json.toString();
    }
}
