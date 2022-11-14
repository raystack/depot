package io.odpf.depot.http.request.builder;

import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class UriBuilder {

    private final String urlConfig;

    public UriBuilder(String urlConfig) {
        this.urlConfig = urlConfig;
    }

    public URI build(Map<String, String> queryParam) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(urlConfig);
        queryParam.forEach(uriBuilder::addParameter);
        return uriBuilder.build();
    }
}
