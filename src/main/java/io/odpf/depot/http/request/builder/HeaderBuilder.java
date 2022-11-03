package io.odpf.depot.http.request.builder;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class HeaderBuilder {

    private final String headerConfig;

    public HeaderBuilder(String headerConfig) {
        this.headerConfig = headerConfig;
    }

    public Map<String, String> build() {
        return Arrays.stream(headerConfig.split(","))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty()).collect(Collectors
                        .toMap(headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
    }
}
