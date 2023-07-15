package org.raystack.depot.message.field;

import org.raystack.depot.message.field.json.JsonFieldFactory;
import org.raystack.depot.message.field.proto.ProtoFieldFactory;
import org.raystack.depot.message.proto.converter.fields.ProtoField;

public class GenericFieldFactory {

    public static GenericField getField(Object field) {
        if (field instanceof ProtoField) {
            return ProtoFieldFactory.getField((ProtoField) field);
        } else {
            return JsonFieldFactory.getField(field);
        }

    }
}
