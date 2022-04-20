package io.odpf.sink.connectors.config.converter;


import io.odpf.sink.connectors.config.enums.InputSchemaDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class InputSchemaDataTypeConverter implements Converter<InputSchemaDataType> {
    @Override
    public InputSchemaDataType convert(Method method, String input) {
        return InputSchemaDataType.valueOf(input.toUpperCase());
    }
}
