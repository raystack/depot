package io.odpf.depot.message.field.proto;

import io.odpf.depot.message.field.GenericField;
import io.odpf.depot.message.proto.converter.fields.DurationProtoField;
import io.odpf.depot.message.proto.converter.fields.MapProtoField;
import io.odpf.depot.message.proto.converter.fields.MessageProtoField;
import io.odpf.depot.message.proto.converter.fields.ProtoField;
import io.odpf.depot.message.proto.converter.fields.StructProtoField;
import io.odpf.depot.message.proto.converter.fields.TimestampProtoField;


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
        if (field instanceof StructProtoField) {
            return new StructField(field.getValue());
        }
        if (field instanceof TimestampProtoField) {
            return new TimeStampField(field.getValue());
        }
        return new DefaultField(field.getValue());
    }
}
