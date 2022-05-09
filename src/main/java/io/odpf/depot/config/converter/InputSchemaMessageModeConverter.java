package io.odpf.depot.config.converter;

import io.odpf.depot.message.InputSchemaMessageMode;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class InputSchemaMessageModeConverter implements Converter<InputSchemaMessageMode> {
    @Override
    public InputSchemaMessageMode convert(Method method, String input) {
        return InputSchemaMessageMode.valueOf(input.toUpperCase());
    }
}
