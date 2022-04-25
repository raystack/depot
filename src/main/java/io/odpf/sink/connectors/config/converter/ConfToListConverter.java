package io.odpf.sink.connectors.config.converter;

import io.odpf.sink.connectors.common.TupleString;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ConfToListConverter implements Converter<TupleString> {
    public static final String ELEMENT_SEPARATOR = ",";
    public static final String VALUE_SEPARATOR = "=";

    @Override
    public TupleString convert(Method method, String input) {
        if (input.isEmpty()) {
            return null;
        }
        String[] split = input.split(VALUE_SEPARATOR);
        return new TupleString(split[0], split[1]);
    }
}
