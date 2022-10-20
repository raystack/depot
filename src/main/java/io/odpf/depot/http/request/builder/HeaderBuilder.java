package io.odpf.depot.http.request.builder;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class HeaderBuilder {

//    private final Map<Template, Template> headerTemplates;
    private final String headerConfig;

    public HeaderBuilder(String headerConfig) {
        this.headerConfig = headerConfig;
    }

    public Map<String, String> build() {
        return Arrays.stream(headerConfig.split(","))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty()).collect(Collectors
                        .toMap(headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
    }

//    public Map<String, String> build(ParsedOdpfMessage message, OdpfMessageSchema schema) {
//        Map<String, String> baseHeader = build();
//        headerTemplates
//                .entrySet()
//                .foreach(templates -> {
//                            String key = templates.getKey().parse(message, schema);
//                            String value = templates.getValue().parse(message, schema);
//                            header.put(key, value);
//                        }
//                );
//        return baseHeader;
//    }
}
