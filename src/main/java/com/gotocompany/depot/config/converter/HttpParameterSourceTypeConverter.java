package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpParameterSourceTypeConverter implements Converter<HttpParameterSourceType> {
    @Override
    public HttpParameterSourceType convert(Method method, String input) {
        return HttpParameterSourceType.valueOf(input.toUpperCase());
    }
}
