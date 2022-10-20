package io.odpf.depot.config.converter;

import io.odpf.depot.http.enums.HttpParameterSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpParameterSourceTypeConverter implements Converter<HttpParameterSourceType> {
    @Override
    public HttpParameterSourceType convert(Method method, String input) {
        return HttpParameterSourceType.valueOf(input.toUpperCase());
    }
}
