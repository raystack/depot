package io.odpf.depot.message.field.proto;

import io.odpf.depot.message.field.DefaultField;
import io.odpf.depot.message.field.GenericField;
import io.odpf.depot.message.proto.converter.fields.DurationProtoField;
import io.odpf.depot.message.proto.converter.fields.MapProtoField;
import io.odpf.depot.message.proto.converter.fields.MessageProtoField;
import io.odpf.depot.message.proto.converter.fields.ProtoField;


public class ProtoFieldFactory {
    public static GenericField getField(ProtoField field) {
        if (field instanceof DurationProtoField) {
            return new DurationField(field.getValue());
        }
        if (field instanceof MessageProtoField) {
            return new MessageField(field.getValue());
        }
        if (field instanceof MapProtoField) {
            return new MapField(field.getValue());
        }
        return new DefaultField(field.getValue());
    }
}
