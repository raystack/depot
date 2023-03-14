package com.gotocompany.depot.message.field.proto;

import com.gotocompany.depot.message.field.GenericField;
import com.gotocompany.depot.message.proto.converter.fields.DurationProtoField;
import com.gotocompany.depot.message.proto.converter.fields.MapProtoField;
import com.gotocompany.depot.message.proto.converter.fields.MessageProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;
import com.gotocompany.depot.message.proto.converter.fields.StructProtoField;
import com.gotocompany.depot.message.proto.converter.fields.TimestampProtoField;


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
