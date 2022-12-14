package io.odpf.depot.common;

import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;

import java.util.HashMap;
import java.util.Map;

public class TemplateUtils {

    public static Map<String, String> parseTemplateMap(Map<Template, Template> templateMap, ParsedOdpfMessage parsedMessage, OdpfMessageSchema schema) {
        Map<String, String> parsedTemplateMap =  new HashMap<>();
        templateMap
                .forEach((k, v) -> {
                    String key = k.parse(parsedMessage, schema);
                    String value = v.parse(parsedMessage, schema);
                    parsedTemplateMap.put(key, value);
                });
        return parsedTemplateMap;
    }
}
