package io.odpf.depot.http.request.builder;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.common.Template;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class UriBuilder {

    private final Template urlTemplate;
    private final SinkConnectorSchemaMessageMode sourceType;
    private final String schemaProtoKeyClass;
    private final String schemaProtoMessageClass;

    public UriBuilder(HttpSinkConfig config) throws InvalidTemplateException {
        this.urlTemplate = new Template(config.getSinkHttpServiceUrl());
        this.sourceType = config.getSinkConnectorSchemaMessageMode();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    public URI build(MessageContainer container, Map<String, String> queryParam) throws IOException {
        if (sourceType == SinkConnectorSchemaMessageMode.LOG_KEY) {
            return build(urlTemplate.parse(
                    container.getParsedLogKey(schemaProtoKeyClass)),
                    queryParam);
        } else {
            return build(urlTemplate.parse(
                    container.getParsedLogMessage(schemaProtoMessageClass)),
                    queryParam);
        }
    }

    public URI build(Map<String, String> queryParam) {
        return build(urlTemplate.getTemplateString(), queryParam);
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
