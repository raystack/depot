package io.odpf.depot.message.field;

import io.odpf.depot.message.field.json.JsonFieldFactory;
import io.odpf.depot.message.field.proto.ProtoFieldFactory;
import io.odpf.depot.message.proto.converter.fields.ProtoField;

public class GenericFieldFactory {

    public static GenericField getField(Object field) {
        if (field instanceof ProtoField) {
            return ProtoFieldFactory.getField((ProtoField) field);
        } else {
            return JsonFieldFactory.getField(field);
        }

    }
}
