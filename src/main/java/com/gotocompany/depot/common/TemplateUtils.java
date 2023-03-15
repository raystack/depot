package com.gotocompany.depot.common;

import com.gotocompany.depot.message.ParsedMessage;

import java.util.Map;
import java.util.stream.Collectors;

public class TemplateUtils {

    public static Map<String, String> parseTemplateMap(Map<Template, Template> templateMap, ParsedMessage parsedMessage) {
        return templateMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().parse(parsedMessage),
                        entry -> entry.getValue().parse(parsedMessage)
                ));
    }
}
