package io.odpf.depot.message.field.json;

import io.odpf.depot.message.field.DefaultField;
import io.odpf.depot.message.field.GenericField;

public class JsonFieldFactory {
    public static GenericField getField(Object field) {
        return new DefaultField(field);
    }
}
