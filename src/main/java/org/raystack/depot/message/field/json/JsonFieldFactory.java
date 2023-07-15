package org.raystack.depot.message.field.json;

import org.raystack.depot.message.field.GenericField;

public class JsonFieldFactory {
    public static GenericField getField(Object field) {
        return new DefaultField(field);
    }
}
