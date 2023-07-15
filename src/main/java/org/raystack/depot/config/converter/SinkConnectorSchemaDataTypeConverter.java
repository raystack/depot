package org.raystack.depot.config.converter;

import org.raystack.depot.config.enums.SinkConnectorSchemaDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class SinkConnectorSchemaDataTypeConverter implements Converter<SinkConnectorSchemaDataType> {
    @Override
    public SinkConnectorSchemaDataType convert(Method method, String input) {
        return SinkConnectorSchemaDataType.valueOf(input.toUpperCase());
    }
}
