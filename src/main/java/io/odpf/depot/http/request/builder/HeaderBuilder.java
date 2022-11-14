package io.odpf.depot.http.request.builder;

import java.util.Map;

public class HeaderBuilder {

    private final Map<String, String> headerConfig;

    public HeaderBuilder(Map<String, String> headerConfig) {
        this.headerConfig = headerConfig;
    }

    public Map<String, String> build() {
        return headerConfig;
    }
}
