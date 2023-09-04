package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class EmptyStringToNull implements Converter<String> {
    @Override
    public String convert(Method method, String input) {
        if (input.isEmpty()) {
            return null;
        }
        return input;
    }
}
