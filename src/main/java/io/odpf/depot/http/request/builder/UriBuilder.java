package io.odpf.depot.http.request.builder;

import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.common.Template;
import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class UriBuilder {

    private final Template urlTemplate;

    public UriBuilder(String urlConfig) {
        this.urlTemplate = new Template(urlConfig);
    }

    public URI build(ParsedOdpfMessage message, OdpfMessageSchema schema, Map<String, String> queryParam) {
        return build(urlTemplate.parse(message, schema), queryParam);
    }

    public URI build(Map<String, String> queryParam) {
        return build(urlTemplate.toString(), queryParam);
    }

    private URI build(String url, Map<String, String> queryParam) {
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            queryParam.forEach(uriBuilder::addParameter);
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new ConfigurationException("Service URL '" + url + "' is invalid");
        }
    }
}
