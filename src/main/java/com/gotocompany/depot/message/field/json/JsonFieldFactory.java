package com.gotocompany.depot.message.field.json;

import com.gotocompany.depot.message.field.GenericField;

public class JsonFieldFactory {
    public static GenericField getField(Object field) {
        return new DefaultField(field);
    }
}
