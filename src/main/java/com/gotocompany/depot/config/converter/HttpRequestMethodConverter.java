package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpRequestMethodConverter implements Converter<HttpRequestMethodType> {

    @Override
    public HttpRequestMethodType convert(Method method, String input) {
        return HttpRequestMethodType.valueOf(input.toUpperCase());
    }
}
