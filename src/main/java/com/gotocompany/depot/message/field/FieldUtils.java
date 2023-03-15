package com.gotocompany.depot.message.field;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FieldUtils {
    private static final Gson GSON = new GsonBuilder().create();

    public static String convertToStringForMessageTypes(Object value, Function<Object, String> toStringFunc) {
        if (value instanceof Collection<?>) {
            return "[" + ((Collection<?>) value)
                    .stream()
                    .map(toStringFunc)
                    .collect(Collectors.joining(",")) + "]";
        }
        return toStringFunc.apply(value);
    }

    /**
     * This method is used to convert default types which string formats are not in json.
     * for example: a list of doubles, strings, enum etc.
     */
    public static String convertToString(Object value) {
        if (value instanceof Collection<?>) {
            return GSON.toJson(value);
        } else {
            return value.toString();
        }
    }

    public static String convertToStringForSpecialTypes(Object value, Function<Object, String> toStringFunc) {
        if (value instanceof Collection<?>) {
            return GSON.toJson(((Collection<?>) value)
                    .stream()
                    .map(toStringFunc)
                    .collect(Collectors.toList()));
        }
        return toStringFunc.apply(value);
    }
}
