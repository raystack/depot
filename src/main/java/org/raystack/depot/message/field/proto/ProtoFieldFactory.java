package org.raystack.depot.message.field.proto;

import org.raystack.depot.message.field.GenericField;
import org.raystack.depot.message.proto.converter.fields.DurationProtoField;
import org.raystack.depot.message.proto.converter.fields.MapProtoField;
import org.raystack.depot.message.proto.converter.fields.MessageProtoField;
import org.raystack.depot.message.proto.converter.fields.ProtoField;
import org.raystack.depot.message.proto.converter.fields.StructProtoField;
import org.raystack.depot.message.proto.converter.fields.TimestampProtoField;

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
