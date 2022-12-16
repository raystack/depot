package io.odpf.depot.common;

import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;

import java.util.Map;
import java.util.stream.Collectors;

public class TemplateUtils {

    public static Map<String, String> parseTemplateMap(Map<Template, Template> templateMap, ParsedOdpfMessage parsedMessage, OdpfMessageSchema schema) {
        return templateMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().parse(parsedMessage, schema),
                        entry -> entry.getValue().parse(parsedMessage, schema)
                ));
    }
}
