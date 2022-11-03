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

    public URI build() throws URISyntaxException {
        return build(urlConfig, null);
    }

    private URI build(String finalUrl, Map<String, String> queryParam) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(finalUrl);
        if (queryParam != null) {
            queryParam.forEach(uriBuilder::addParameter);
        }
        return uriBuilder.build();
    }
}
