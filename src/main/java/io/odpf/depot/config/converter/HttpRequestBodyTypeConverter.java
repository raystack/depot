package io.odpf.depot.config.converter;

import io.odpf.depot.http.enums.HttpRequestBodyType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpRequestBodyTypeConverter implements Converter<HttpRequestBodyType> {

    @Override
    public HttpRequestBodyType convert(Method method, String input) {
        return HttpRequestBodyType.valueOf(input.toUpperCase());
    }
}
