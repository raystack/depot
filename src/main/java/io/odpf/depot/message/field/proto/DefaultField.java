package io.odpf.depot.message.field.proto;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Message;
import io.odpf.depot.message.field.GenericField;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultField implements GenericField {
    private static final Gson GSON = new GsonBuilder().create();
    private final Object value;

    public DefaultField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        if (value instanceof Collection<?>) {
            if (((List<?>) value).get(0) instanceof Message) {
                Object messageJsons = ((Collection<?>) value)
                        .stream()
                        .map(cValue -> new MessageField(cValue).getString())
                        .collect(Collectors.joining(","));
                return "[" + messageJsons + "]";
            } else {
                return GSON.toJson(((Collection<?>) value).stream().map(Object::toString).collect(Collectors.toList()));
            }
        } else {
            return value.toString();
        }
    }
}
