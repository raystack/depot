package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class SinkConnectorSchemaMessageModeConverter implements Converter<SinkConnectorSchemaMessageMode> {
    @Override
    public SinkConnectorSchemaMessageMode convert(Method method, String input) {
        return SinkConnectorSchemaMessageMode.valueOf(input.toUpperCase());
    }
}
