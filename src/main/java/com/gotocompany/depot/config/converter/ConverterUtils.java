package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.Tuple;

import java.util.ArrayList;
import java.util.List;

public class ConverterUtils {
    public static final String ELEMENT_SEPARATOR = ",";
    private static final String VALUE_SEPARATOR = "=";
    private static final int MAX_LENGTH = 63;

    public static List<Tuple<String, String>> convertToList(String input) {
        List<Tuple<String, String>> result = new ArrayList<>();
        String[] chunks = input.split(ELEMENT_SEPARATOR, -1);
        for (String chunk : chunks) {
            String[] entry = chunk.split(VALUE_SEPARATOR, -1);
            if (entry.length <= 1) {
                continue;
            }
            String key = entry[0].trim();
            if (key.isEmpty()) {
                continue;
            }

            String value = entry[1].trim();
            value = value.length() > MAX_LENGTH ? value.substring(0, MAX_LENGTH) : value;
            result.add(new Tuple<>(key, value));
        }
        return result;
    }
}
