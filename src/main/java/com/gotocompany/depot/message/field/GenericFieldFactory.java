package com.gotocompany.depot.message.field;

import com.gotocompany.depot.message.field.json.JsonFieldFactory;
import com.gotocompany.depot.message.field.proto.ProtoFieldFactory;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;

public class GenericFieldFactory {

    public static GenericField getField(Object field) {
        if (field instanceof ProtoField) {
            return ProtoFieldFactory.getField((ProtoField) field);
        } else {
            return JsonFieldFactory.getField(field);
        }

    }
}
