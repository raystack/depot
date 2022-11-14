package io.odpf.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpHeaderConverter implements Converter<Map<String, String>> {

    private static final String ELEMENT_SEPARATOR = ",";

    private static final String VALUE_SEPARATOR = ":";

    @Override
    public Map<String, String> convert(Method method, String input) {
        return Arrays.stream(input.split(ELEMENT_SEPARATOR))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty())
                .collect(Collectors.toMap(headerKeyValue -> headerKeyValue.split(VALUE_SEPARATOR)[0],
                        headerKeyValue -> headerKeyValue.split(VALUE_SEPARATOR)[1]));
    }
}
